//! Transfer sequencing reads to a receiver and report validation progress.
//!
//! This prototype does not sketch reads or produce a metagenomic profile.

mod client;
mod protocol;
mod receiver;

use serde::{Deserialize, Serialize};

pub use client::Client;
pub use receiver::Receiver;

/// An input, connection, or protocol error reported by the client.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Bounds for one decoded read batch, not the total upload.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub(crate) batch_bytes: usize,
    pub(crate) batch_reads: usize,
}

impl Limits {
    /// Set the maximum sequence, quality, and identifier bytes and read count.
    ///
    /// # Errors
    /// Returns an error for zero limits or limits above the prototype's
    /// supported maxima: 1 MiB of data and 16,384 reads per batch.
    pub fn new(batch_bytes: usize, batch_reads: usize) -> Result<Self, Error> {
        if !(1..=1_048_576).contains(&batch_bytes) || !(1..=16_384).contains(&batch_reads) {
            return Err("batch limits must be 1..=1048576 bytes and 1..=16384 reads".into());
        }
        Ok(Self {
            batch_bytes,
            batch_reads,
        })
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            batch_bytes: 1_048_576,
            batch_reads: 1024,
        }
    }
}

/// Cumulative validation counts for one upload, not profiling or durability.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    /// Number of reads validated by the receiver.
    pub reads: u64,
    /// Number of sequence bases validated by the receiver.
    pub bases: u64,
    /// True at request EOF. The client also checks its input before reporting it.
    pub complete: bool,
}
