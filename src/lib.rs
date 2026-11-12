//! Transfer sequencing reads and observe cumulative in-memory sylph profiles.
//!
//! Uploads share a receiver's fixed scientific parameters and duplicate history.
//! [`Session`] adds periodic profiles; [`Receiver`] also supports sketch-only use.
//! Neither interface retains raw reads or persists session state.

mod client;
mod error;
mod protocol;
mod receiver;
mod session;

use serde::{Deserialize, Serialize};

pub use client::Client;
pub use error::Error;
pub use receiver::Receiver;
pub use session::{
    Observation, ProcessedCheckpoint, ProfileSnapshot, RefreshFailure, Session, SessionBuilder,
    SessionState,
};

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
            return Err(Error::InvalidLimits {
                batch_bytes,
                batch_reads,
            });
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

/// Cumulative validation counts for one upload, not the sample or durable storage.
///
/// The receiver emits a batch receipt after adding that batch to its sketch.
/// Missing receipts do not imply rejection: cancellation can lose a receipt
/// for accepted data. These counts do not make retries safe.
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
