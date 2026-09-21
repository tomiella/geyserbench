use anyhow::{Context, ensure};
use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;
use std::{collections::HashMap, error::Error, sync::atomic::Ordering};
use tokio::task;
use tracing::{Level, info, trace, warn};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
};

#[allow(clippy::all, dead_code)]
pub mod shreder_binary {
    include!(concat!(env!("OUT_DIR"), "/shreder_binary.rs"));
}

use shreder_binary::{
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
    shreder_binary_service_client::ShrederBinaryServiceClient,
};

// Decode delivery data without sanitize(): the ordinary Shreder provider also
// includes transactions that may fail execution or account-index validation.
fn decode_binary_transaction(
    raw: &[u8],
    signatures: &[Vec<u8>],
) -> anyhow::Result<VersionedTransaction> {
    let mut remaining = raw;
    let transaction: VersionedTransaction =
        wincode::deserialize_from(&mut remaining).context("invalid transaction wire data")?;
    ensure!(remaining.is_empty(), "trailing transaction bytes");
    let signature = transaction
        .signatures
        .first()
        .context("transaction has no signature")?;
    let envelope_signature = signatures
        .first()
        .context("binary envelope has no signature")?;
    ensure!(
        signature.as_ref() == envelope_signature.as_slice(),
        "binary envelope signature does not match transaction"
    );
    Ok(transaction)
}

pub struct ShrederBinaryProvider;

impl GeyserProvider for ShrederBinaryProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_shreder_binary_endpoint(endpoint, config, context).await })
    }
}

async fn process_shreder_binary_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;
    let signature_sender = signature_tx;
    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client = ShrederBinaryServiceClient::connect(endpoint_url.clone())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let mut transactions: HashMap<String, SubscribeRequestFilterBinaryTransactions> =
        HashMap::with_capacity(1);
    transactions.insert(
        String::from("account"),
        SubscribeRequestFilterBinaryTransactions {
            account_exclude: vec![],
            account_include: vec![],
            account_required: vec![config.account.clone()],
        },
    );

    let request = SubscribeBinaryTransactionsRequest { transactions };
    let (mut subscribe_tx, subscribe_rx) =
        unbounded::<shreder_binary::SubscribeBinaryTransactionsRequest>();
    subscribe_tx.send(request).await?;
    let mut stream = client
        .subscribe_binary_transactions(subscribe_rx)
        .await?
        .into_inner();

    let mut accumulator = TransactionAccumulator::new();

    let mut transaction_count = 0usize;
    let mut decode_errors = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                if let Some(m) = message.as_ref() {
                    trace!(endpoint = %endpoint_name, ?m, "Received stream message");
                }

                let Some(Ok(msg)) = message else { continue };
                let Some(tx_update) = msg.transaction.as_ref() else { continue };
                let Some(tx) = tx_update.transaction.as_ref() else { continue };

                let raw = &tx.binary_transaction;
                let versioned_tx = match decode_binary_transaction(raw, &tx.signatures) {
                    Ok(transaction) => transaction,
                    Err(err) => {
                        decode_errors += 1;
                        if decode_errors == 1 {
                            warn!(endpoint = %endpoint_name, slot = tx_update.slot, error = %format_args!("{err:#}"),
                                "Skipping invalid binary transaction");
                        }
                        continue;
                    }
                };

                let has_account = versioned_tx
                    .message
                    .static_account_keys()
                    .iter()
                    .any(|k| k.to_bytes() == account_pubkey.to_bytes());
                if !has_account { continue }

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();
                let signature = tx
                    .signatures
                    .first()
                    .map(|s| bs58::encode(s).into_string())
                    .unwrap_or_default();

                if let Some(file) = log_file.as_mut() {
                    write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                }

                let tx_data = TransactionData {
                    wallclock_secs: wallclock,
                    elapsed_since_start: elapsed,
                    start_wallclock_secs,
                };

                let updated = accumulator.record(signature.clone(), tx_data.clone());

                if updated && let Some(envelope) = build_signature_envelope(
                    &comparator,
                    &endpoint_name,
                    &signature,
                    tx_data,
                    total_producers,
                ) {
                    if let Some(target) = target_transactions {
                        let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                        if let Some(tracker) = progress.as_ref() {
                            tracker.record(shared);
                        }
                        if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                            info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                            let _ = shutdown_tx.send(());
                        }
                    }

                    if let Some(sender) = signature_sender.as_ref() {
                        enqueue_signature(sender, &endpoint_name, &signature, envelope);
                    }
                }

                transaction_count += 1;
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        decode_errors,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_message::VersionedMessage;

    const SIGNATURE: [u8; 64] = [9; 64];
    const PAYER: [u8; 32] = [1; 32];
    const PROGRAM: [u8; 32] = [2; 32];

    // Fixed wire fixtures independent of the decoder's serializer.
    fn legacy_or_v0(versioned: bool, lookups: bool) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend(SIGNATURE);
        if versioned {
            bytes.push(0x80);
        }
        bytes.extend([1, 0, 1, 2]); // header and account count
        bytes.extend(PAYER);
        bytes.extend(PROGRAM);
        bytes.extend([3; 32]); // blockhash
        bytes.extend([1, 1]); // instruction count and program index
        if lookups {
            bytes.extend([3, 0, 2, 3]); // payer and two loaded accounts
        } else {
            bytes.extend([1, 0]);
        }
        bytes.extend([1, 42]); // instruction data
        if versioned {
            bytes.push(u8::from(lookups));
            if lookups {
                bytes.extend([4; 32]); // table address
                bytes.extend([1, 0, 1, 1]); // writable/readonly indices
            }
        }
        bytes
    }

    fn v1(data_len: u16) -> Vec<u8> {
        let mut bytes = vec![0x81, 1, 0, 1]; // version, then message header
        bytes.extend(0u32.to_le_bytes()); // transaction config
        bytes.extend([3; 32]); // lifetime specifier
        bytes.extend([1, 2]); // instruction count, account count
        bytes.extend(PAYER);
        bytes.extend(PROGRAM);
        bytes.extend([1, 1]); // program index, instruction account count
        bytes.extend(data_len.to_le_bytes());
        bytes.push(0); // payer account index
        bytes.extend(vec![42; usize::from(data_len)]);
        bytes.extend(SIGNATURE); // V1 signatures follow the message
        bytes
    }

    fn fixtures() -> Vec<Vec<u8>> {
        vec![
            legacy_or_v0(false, false),
            legacy_or_v0(true, false),
            legacy_or_v0(true, true),
            v1(1),
            v1(1300),
        ]
    }

    #[test]
    fn decodes_legacy_v0_and_v1_with_static_account_filtering() {
        let filter = Pubkey::new_from_array(PROGRAM);
        for bytes in fixtures() {
            let decoded = decode_binary_transaction(&bytes, &[SIGNATURE.to_vec()]).unwrap();
            assert_eq!(decoded.signatures[0].as_ref(), SIGNATURE);
            assert!(
                decoded
                    .message
                    .static_account_keys()
                    .iter()
                    .any(|key| key.to_bytes() == filter.to_bytes())
            );
            assert!(
                !decoded
                    .message
                    .static_account_keys()
                    .iter()
                    .any(|key| key.to_bytes() == [5; 32])
            );
            assert_eq!(wincode::serialize(&decoded).unwrap(), bytes);
        }
        let decoded = decode_binary_transaction(&v1(1300), &[SIGNATURE.to_vec()]).unwrap();
        assert!(v1(1300).len() > 1232);
        assert!(matches!(decoded.message, VersionedMessage::V1(_)));
        let decoded =
            decode_binary_transaction(&legacy_or_v0(true, true), &[SIGNATURE.to_vec()]).unwrap();
        assert_eq!(decoded.message.address_table_lookups().unwrap().len(), 1);
    }

    #[test]
    fn rejects_empty_truncated_trailing_and_unknown_wire_data() {
        for bytes in fixtures() {
            for end in 0..bytes.len() {
                assert!(
                    decode_binary_transaction(&bytes[..end], &[SIGNATURE.to_vec()]).is_err(),
                    "accepted truncated transaction at byte {end}"
                );
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert!(decode_binary_transaction(&trailing, &[SIGNATURE.to_vec()]).is_err());
        }
        assert!(decode_binary_transaction(&[0x82], &[SIGNATURE.to_vec()]).is_err());
        let mut unknown = legacy_or_v0(true, false);
        unknown[65] = 0x82;
        assert!(decode_binary_transaction(&unknown, &[SIGNATURE.to_vec()]).is_err());
    }

    #[test]
    fn rejects_missing_or_mismatched_signatures() {
        for bytes in fixtures() {
            assert!(decode_binary_transaction(&bytes, &[]).is_err());
            assert!(decode_binary_transaction(&bytes, &[vec![9; 63]]).is_err());
            assert!(decode_binary_transaction(&bytes, &[vec![8; 64]]).is_err());
        }
        let signed = legacy_or_v0(false, false);
        let mut unsigned = vec![0];
        unsigned.extend_from_slice(&signed[65..]);
        assert!(decode_binary_transaction(&unsigned, &[SIGNATURE.to_vec()]).is_err());
    }

    #[test]
    fn preserves_delivery_of_transactions_with_invalid_instruction_indices() {
        let mut bytes = v1(1);
        bytes[106] = 2; // program index outside static accounts
        let decoded = decode_binary_transaction(&bytes, &[SIGNATURE.to_vec()]).unwrap();
        assert!(decoded.sanitize().is_err());
    }
}
