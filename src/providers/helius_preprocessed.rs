use futures::SinkExt;
use futures_util::stream::StreamExt;
use std::{error::Error, sync::atomic::Ordering, time::Duration};
use tokio::{task, time::interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{Level, info, warn};

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

// Binary frame layout: version (u8) | slot (u64 LE) | signature ([u8; 64]) | wire transaction.
const PREFIX_LEN: usize = 73;
const PAYLOAD_VERSION: u8 = 1;

fn frame_signature(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() <= PREFIX_LEN || frame[0] != PAYLOAD_VERSION {
        return None;
    }
    Some(&frame[9..PREFIX_LEN])
}

pub struct HeliusPreprocessedProvider;

impl GeyserProvider for HeliusPreprocessedProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_helius_preprocessed_endpoint(endpoint, config, context).await })
    }
}

async fn process_helius_preprocessed_endpoint(
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
    let endpoint_name = endpoint.name.clone();

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    // Accept either a full URL with ?api-key= or a bare URL plus x_token as the key.
    let mut url = url::Url::parse(&endpoint.url)?;
    if let Some(key) = endpoint.x_token.as_ref()
        && !url.query_pairs().any(|(k, _)| k == "api-key")
    {
        url.query_pairs_mut().append_pair("api-key", key);
    }

    info!(endpoint = %endpoint_name, url = %endpoint.url, "Connecting");
    let (ws, _) = connect_async(url.as_str())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");
    let (mut sink, mut stream) = ws.split();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "preprocessedSubscribe",
        "params": {
            "accountInclude": [config.account.clone()],
            "accountExclude": [],
            "accountRequired": [],
        }
    });
    sink.send(Message::Text(request.to_string())).await?;

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut invalid_frames = 0usize;
    let mut ping = interval(Duration::from_secs(30));
    ping.tick().await;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            _ = ping.tick() => {
                sink.send(Message::Ping(Vec::new())).await?;
            }

            message = stream.next() => {
                let frame = match message {
                    Some(Ok(Message::Binary(frame))) => frame,
                    Some(Ok(Message::Text(text))) => {
                        info!(endpoint = %endpoint_name, %text, "Text frame");
                        continue;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!(endpoint = %endpoint_name, ?frame, "Server closed connection");
                        break;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(err)) => {
                        warn!(endpoint = %endpoint_name, error = %err, "WebSocket error");
                        break;
                    }
                    None => break,
                };

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();

                let Some(sig_bytes) = frame_signature(&frame) else {
                    invalid_frames += 1;
                    if invalid_frames == 1 {
                        warn!(endpoint = %endpoint_name, len = frame.len(), version = frame.first().copied(),
                            "Skipping unrecognized binary frame");
                    }
                    continue;
                };
                let signature = bs58::encode(sig_bytes).into_string();

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

                    if let Some(sender) = signature_tx.as_ref() {
                        enqueue_signature(sender, &endpoint_name, &signature, envelope);
                    }
                }

                transaction_count += 1;
            }
        }
    }

    let unique_signatures = accumulator.len();
    comparator.add_batch(&endpoint_name, accumulator.into_inner());
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        invalid_frames,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}
