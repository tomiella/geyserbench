use futures_util::stream::StreamExt;
use std::{error::Error, sync::atomic::Ordering};
use tokio::task;
use tracing::{Level, error, info};

use solana_pubkey::Pubkey;

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
pub mod shredstream {
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub struct ShredstreamProvider;

fn decode_entries(bytes: &[u8]) -> wincode::ReadResult<Vec<solana_entry::entry::Entry>> {
    wincode::deserialize_exact(bytes)
}

impl GeyserProvider for ShredstreamProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_shredstream_endpoint(endpoint, config, context).await })
    }
}

async fn process_shredstream_endpoint(
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

    let mut client = shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(
        endpoint_url.clone(),
    )
    .await
    .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let request = shredstream::SubscribeEntriesRequest {};
    let mut stream = client.subscribe_entries(request).await?.into_inner();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
        _ = shutdown_rx.recv() => {
            info!(endpoint = %endpoint_name, "Received stop signal");
            break;
        }

        Some(Ok(slot_entry)) = stream.next() => {
            let entries = match decode_entries(&slot_entry.entries) {
                Ok(e) => e,
                Err(e) => {
                    error!(endpoint = %endpoint_name, error = %e, "Failed to deserialize shredstream entries");
                    continue;
                }
            };
            for entry in entries {
                for tx in entry.transactions {
                    let has_account = tx
                        .message
                        .static_account_keys()
                        .iter()
                        .any(|key| key.as_ref() == account_pubkey.as_ref());

                    if !has_account {
                        continue;
                    }

                    let wallclock = get_current_timestamp();
                    let elapsed = start_instant.elapsed();
                    let Some(signature) = tx.signatures.first() else {
                        error!(endpoint = %endpoint_name, "Missing signature in shredstream transaction");
                        continue;
                    };
                    let signature = signature.to_string();

                    if let Some(file) = log_file.as_mut() {
                        write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                    }

                    let tx_data = TransactionData {
                        wallclock_secs: wallclock,
                        elapsed_since_start: elapsed,
                        start_wallclock_secs,
                    };

                    let updated = accumulator.record(
                        signature.clone(),
                        tx_data.clone(),
                    );

                    if updated
                        && let Some(envelope) = build_signature_envelope(
                            &comparator,
                            &endpoint_name,
                            &signature,
                            tx_data,
                            total_producers,
                        ) {
                            if let Some(target) = target_transactions {
                                let shared = shared_counter
                                    .fetch_add(1, Ordering::AcqRel)
                                    + 1;
                                if let Some(tracker) = progress.as_ref() {
                                    tracker.record(shared);
                                }
                                if shared >= target
                                    && !shared_shutdown.swap(true, Ordering::AcqRel)
                                {
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
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_message::VersionedMessage;

    const PAYER: [u8; 32] = [1; 32];
    const PROGRAM: [u8; 32] = [2; 32];
    const SIGNATURE: [u8; 64] = [9; 64];

    // Independent wire fixtures also check compatibility with the original
    // bincode entry framing: u64 vector lengths, num_hashes, then 32 hash bytes.
    fn entries_fixture(entries: &[Vec<Vec<u8>>]) -> Vec<u8> {
        let mut bytes = (entries.len() as u64).to_le_bytes().to_vec();
        for (index, transactions) in entries.iter().enumerate() {
            bytes.extend((index as u64 + 1).to_le_bytes());
            bytes.extend([index as u8 + 3; 32]);
            bytes.extend((transactions.len() as u64).to_le_bytes());
            for transaction in transactions {
                bytes.extend(transaction);
            }
        }
        bytes
    }

    fn legacy_or_v0(versioned: bool, lookups: bool) -> Vec<u8> {
        let mut bytes = vec![1]; // short_vec signature count
        bytes.extend(SIGNATURE);
        if versioned {
            bytes.push(0x80);
        }
        bytes.extend([1, 0, 1, 2]); // message header and account count
        bytes.extend(PAYER);
        bytes.extend(PROGRAM);
        bytes.extend([3; 32]); // blockhash
        bytes.extend([1, 1]); // instruction count and program index
        if lookups {
            bytes.extend([3, 0, 2, 3]); // static payer and two loaded accounts
        } else {
            bytes.extend([1, 0]);
        }
        bytes.extend([1, 42]); // data length and instruction data
        if versioned {
            bytes.push(u8::from(lookups));
            if lookups {
                bytes.extend([4; 32]);
                bytes.extend([1, 0, 1, 1]); // writable and readonly table indices
            }
        }
        bytes
    }

    fn v1(data_len: u16) -> Vec<u8> {
        let mut bytes = vec![0x81, 1, 0, 1];
        bytes.extend(0u32.to_le_bytes()); // empty transaction config
        bytes.extend([3; 32]); // lifetime specifier
        bytes.extend([1, 2]); // instruction and address counts
        bytes.extend(PAYER);
        bytes.extend(PROGRAM);
        bytes.extend([1, 1]); // program index and instruction account count
        bytes.extend(data_len.to_le_bytes());
        bytes.push(0); // payer account index
        bytes.extend(vec![42; usize::from(data_len)]);
        bytes.extend(SIGNATURE); // V1 puts signatures after the message
        bytes
    }

    #[test]
    fn decodes_mixed_versions_across_entries_including_large_v1() {
        let large_v1 = v1(1300);
        assert!(large_v1.len() > 1232);
        let bytes = entries_fixture(&[
            vec![legacy_or_v0(false, false), legacy_or_v0(true, false)],
            vec![], // tick entry between transaction batches
            vec![legacy_or_v0(true, true), v1(1), large_v1],
        ]);
        let entries = decode_entries(&bytes).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].num_hashes, 1);
        assert_eq!(entries[1].num_hashes, 2);
        assert_eq!(entries[2].num_hashes, 3);
        assert!(entries[1].transactions.is_empty());
        assert_eq!(entries[2].hash.as_ref(), &[5; 32]);
        assert!(matches!(
            entries[0].transactions[0].message,
            VersionedMessage::Legacy(_)
        ));
        assert!(matches!(
            entries[0].transactions[1].message,
            VersionedMessage::V0(_)
        ));
        let VersionedMessage::V0(with_lookups) = &entries[2].transactions[0].message else {
            panic!("expected V0");
        };
        assert_eq!(with_lookups.address_table_lookups.len(), 1);
        for (tx, expected_len) in entries[2].transactions[1..].iter().zip([1, 1300]) {
            let VersionedMessage::V1(message) = &tx.message else {
                panic!("expected V1");
            };
            assert_eq!(message.instructions[0].data.len(), expected_len);
            assert_eq!(message.instructions[0].program_id_index, 1);
            assert_eq!(message.instructions[0].accounts, [0]);
        }
        for tx in entries.iter().flat_map(|entry| &entry.transactions) {
            assert_eq!(tx.signatures[0].as_ref(), SIGNATURE);
            let keys = tx.message.static_account_keys();
            assert_eq!(keys[0].as_ref(), PAYER);
            assert_eq!(keys[1].as_ref(), PROGRAM);
        }
        assert_eq!(wincode::serialize(&entries).unwrap(), bytes);
    }

    #[test]
    fn accepts_empty_entry_vector_but_rejects_missing_or_truncated_bytes() {
        assert!(decode_entries(&0u64.to_le_bytes()).unwrap().is_empty());
        let bytes = entries_fixture(&[vec![legacy_or_v0(false, false), v1(1300)]]);
        for length in 0..bytes.len() {
            assert!(
                decode_entries(&bytes[..length]).is_err(),
                "accepted truncation at {length}"
            );
        }
    }

    #[test]
    fn rejects_unknown_versions_bad_framing_and_trailing_bytes() {
        assert!(decode_entries(&entries_fixture(&[vec![vec![0x82]]])).is_err());
        assert!(decode_entries(&u64::MAX.to_le_bytes()).is_err());

        let mut bytes = entries_fixture(&[vec![v1(1)]]);
        bytes[48..56].copy_from_slice(&u64::MAX.to_le_bytes()); // invalid transaction count
        assert!(decode_entries(&bytes).is_err());

        let mut bytes = entries_fixture(&[vec![v1(1)]]);
        bytes.push(0);
        assert!(decode_entries(&bytes).is_err());
    }

    #[test]
    fn decodes_delivered_transactions_without_runtime_sanitization() {
        let mut transaction = legacy_or_v0(false, false);
        transaction[166] = 250; // program index outside the account list
        let entries = decode_entries(&entries_fixture(&[vec![transaction]])).unwrap();
        assert_eq!(entries[0].transactions.len(), 1);
        assert!(entries[0].transactions[0].sanitize().is_err());
    }
}
