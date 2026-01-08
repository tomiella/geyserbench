use std::{collections::HashMap, sync::Arc};

use crossbeam_queue::ArrayQueue;
use tracing::{error, warn};

use crate::utils::{Comparator, EventData};

#[derive(Default)]
pub struct TransactionAccumulator {
    entries: HashMap<String, EventData>,
}

impl TransactionAccumulator {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn record(&mut self, signature: String, data: EventData) -> bool {
        use std::collections::hash_map::Entry;

        match self.entries.entry(signature) {
            Entry::Vacant(entry) => {
                entry.insert(data);
                true
            }
            Entry::Occupied(mut entry) => {
                if data.elapsed_since_start < entry.get().elapsed_since_start {
                    entry.insert(data);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn into_inner(self) -> HashMap<String, EventData> {
        self.entries
    }
}

pub fn fatal_connection_error(endpoint: &str, err: impl std::fmt::Display) -> ! {
    error!(endpoint = endpoint, error = %err, "Failed to connect to endpoint");
    eprintln!("Failed to connect to endpoint {}: {}", endpoint, err);
    std::process::exit(1);
}

pub fn build_signature_envelope(
    comparator: &Arc<Comparator>,
    endpoint: &str,
    signature: &str,
    data: EventData,
    total_producers: usize,
) {
    comparator.record_observation(endpoint, signature, data, total_producers);
}
