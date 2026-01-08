pub use {
    bs58,
    bytes::Bytes,
    futures_util::stream::StreamExt,
    serde::{Deserialize, Serialize},
    std::{
        env,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
};

mod analysis;
mod config;
mod proto;
mod providers;
mod utils;

use anyhow::{anyhow, Result};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use utils::{get_current_timestamp, Comparator, ProgressTracker};
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const DEFAULT_BACKEND_STREAM_URL: &str = "wss://gb.solstack.app/v1/benchmarks/stream";
const MAX_STREAM_EVENTS: i32 = 100_000;
const SIGNATURE_QUEUE_CAPACITY: usize = 1_024;

struct CliArgs {
    config_path: Option<String>,
    disable_streaming: bool,
}

impl CliArgs {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = CliArgs {
            config_path: None,
            disable_streaming: false,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let value = args.next().unwrap_or_else(|| {
                        eprintln!("Missing value for --config");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.config_path = Some(value);
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("Unknown argument: {}", other);
                    print_usage();
                    std::process::exit(1);
                }
            }
        }

        parsed
    }
}

fn print_usage() {
    eprintln!("Usage: geyserbench [--config <PATH>] [--private]");
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|err| anyhow!(err))?;

    let cli = CliArgs::parse();
    let config_path = cli.config_path.as_deref().unwrap_or(DEFAULT_CONFIG_PATH);
    let config = config::ConfigToml::load_or_create(config_path)?;
    info!(config_path = config_path, "Loaded configuration");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let start_time_local = get_current_timestamp();
    let comparator = Arc::new(Comparator::new());
    let start_instant = Instant::now();
    let clock_offset_ms: f64;
    let server_started_at_unix_ms: Option<i64>;
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));

    let mut signature_forwarder: Option<thread::JoinHandle<()>> = None;
    let mut forwarder_stop: Option<Arc<AtomicBool>> = None;

    let mut handles = Vec::new();
    let endpoint_names: Vec<String> = config.endpoint.iter().map(|e| e.name.clone()).collect();
    let global_target = if config.config.events > 0 {
        Some(config.config.events as usize)
    } else {
        None
    };
    let progress_tracker = global_target.map(|target| Arc::new(ProgressTracker::new(target)));

    let total_producers = config.endpoint.len();
    for (index, endpoint) in config.endpoint.clone().into_iter().enumerate() {
        let provider = providers::create_provider(&endpoint.kind);
        let shared_config = config.config.clone();
        let context = providers::ProviderContext {
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx: shutdown_tx.subscribe(),
            start_wallclock_secs: start_time_local,
            start_instant,
            comparator: comparator.clone(),
            shared_counter: shared_counter.clone(),
            shared_shutdown: shared_shutdown.clone(),
            target_events: global_target,
            total_producers,
            progress: progress_tracker.clone(),
        };

        handles.push(provider.process(endpoint, shared_config, context));
    }

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let shared_shutdown = shared_shutdown.clone();
        let aborted = aborted.clone();
        async move {
            match ctrl_c().await {
                Ok(()) => {
                    let already_aborting = aborted.swap(true, Ordering::AcqRel);
                    if already_aborting {
                        info!("Received additional Ctrl+C; shutdown already in progress");
                    } else {
                        info!("Received Ctrl+C; initiating shutdown");
                    }
                    shared_shutdown.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(());
                }
                Err(err) => error!(error = %err, "Failed to listen for Ctrl+C"),
            }
        }
    });

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => error!(error = ?e, "Provider task returned error"),
            Err(e) => error!(error = ?e, "Provider join error"),
        }
    }

    let run_aborted = aborted.load(Ordering::Acquire);

    let run_summary = if !run_aborted {
        Some(analysis::compute_run_summary(
            comparator.as_ref(),
            &endpoint_names,
        ))
    } else {
        None
    };

    if let Some(stop) = forwarder_stop.as_ref() {
        stop.store(true, Ordering::Release);
    }

    if let Some(join) = signature_forwarder
        && let Err(err) = join.join()
    {
        warn!(
            "Signature forwarder thread terminated unexpectedly: {:?}",
            err
        );
    }

    if !run_aborted {
        if let Some(summary) = run_summary.as_ref() {
            analysis::display_run_summary(summary);
            let metrics_json = analysis::build_metrics_report(summary);
            debug!(metrics = %metrics_json, "Computed run metrics");
        }
    } else {
        info!("Benchmark aborted before completion; no results were generated");
    }

    Ok(())
}
