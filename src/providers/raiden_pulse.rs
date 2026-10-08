use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
use std::{collections::HashMap, error::Error, sync::atomic::Ordering};
use tokio::task;
use tracing::{Level, info, trace, warn};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    shreder_binary::decode_binary_transaction,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
};

#[allow(clippy::all, dead_code)]
pub mod raiden_binary {
    include!(concat!(env!("OUT_DIR"), "/raiden_binary.rs"));
}

use raiden_binary::{
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
    raiden_binary_service_client::RaidenBinaryServiceClient,
};

pub struct RaidenPulseProvider;

impl GeyserProvider for RaidenPulseProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_raiden_pulse_endpoint(endpoint, config, context).await })
    }
}

async fn process_raiden_pulse_endpoint(
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

    let mut client = RaidenBinaryServiceClient::connect(endpoint_url.clone())
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
        unbounded::<raiden_binary::SubscribeBinaryTransactionsRequest>();
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

                // Pulse V2 resolves lookup tables server-side, so loaded accounts count too.
                let target = account_pubkey.to_bytes();
                let has_account = versioned_tx
                    .message
                    .static_account_keys()
                    .iter()
                    .any(|k| k.to_bytes() == target)
                    || tx_update
                        .loaded_writable_addresses
                        .iter()
                        .chain(&tx_update.loaded_readonly_addresses)
                        .any(|k| k.as_slice() == target);
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

