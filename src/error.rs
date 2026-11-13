use std::io;

use arrow_flight::error::FlightError;
use arrow_schema::ArrowError;
use seq_io::fastq;
use sylph::{profile::ProfileError, read_sketch::SketchError};
use thiserror::Error;
use tokio::task::JoinError;
use tonic::transport;

use crate::Progress;

/// An input, connection, protocol, or analysis-session error.
#[derive(Debug, Error)]
pub enum Error {
    /// Batch limits are zero or exceed the supported maxima.
    #[error(
        "batch limits must be 1..=1048576 bytes and 1..=16384 reads; got {batch_bytes} bytes and {batch_reads} reads"
    )]
    InvalidLimits {
        /// Requested data bytes per batch.
        batch_bytes: usize,
        /// Requested reads per batch.
        batch_reads: usize,
    },
    /// A read cannot fit in one batch.
    #[error("one read contains {bytes} data bytes, exceeding the batch limit of {limit}")]
    ReadTooLarge {
        /// Combined identifier, sequence, and quality bytes.
        bytes: usize,
        /// Configured bytes per batch.
        limit: usize,
    },
    /// A size or counter cannot be represented.
    #[error("{0} overflow")]
    Overflow(&'static str),
    /// The upload closed before the parser could send its input.
    #[error("receiver closed the upload")]
    UploadClosed,
    /// Progress moved backwards or changed after completion.
    #[error("receiver returned inconsistent progress")]
    InconsistentProgress,
    /// The response stream ended without a completion receipt.
    #[error("upload ended without a completion receipt")]
    MissingCompletion,
    /// Receiver totals differ from the parsed input.
    #[error("receiver counts ({} reads, {} bases) do not match the input ({} reads, {} bases)", received.reads, received.bases, expected.reads, expected.bases)]
    CountMismatch {
        /// Totals from the local parser.
        expected: Progress,
        /// Totals from the receiver.
        received: Progress,
    },
    /// Reading the input failed.
    #[error(transparent)]
    Input(#[from] io::Error),
    /// FASTQ parsing failed, including parser-buffer and decompression errors.
    #[error(transparent)]
    Fastq(#[from] fastq::Error),
    /// Building an Arrow batch failed.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Creating an endpoint or connecting to it failed.
    #[error(transparent)]
    Connection(#[from] transport::Error),
    /// Flight encoding, transfer, or remote validation failed.
    #[error(transparent)]
    Flight(#[from] FlightError),
    /// A receiver receipt could not be decoded.
    #[error(transparent)]
    Progress(#[from] serde_json::Error),
    /// The blocking parser task panicked or was cancelled.
    #[error(transparent)]
    ParserTask(#[from] JoinError),
    /// A receiver's blocking task panicked or was cancelled.
    #[error("receiver task failed: {0}")]
    ReceiverTask(#[source] JoinError),
    /// A panic while holding the sketch lock made its state unsafe to reuse.
    #[error("sketch state is poisoned")]
    SketchPoisoned,
    /// Refresh eligibility must be positive and fit the monotonic clock.
    #[error("refresh interval must be positive and representable")]
    InvalidRefreshInterval,
    /// Starting a session requires a running Tokio runtime.
    #[error("session startup requires a Tokio runtime")]
    MissingRuntime,
    /// Shutdown closed the session without requesting a final result.
    #[error("session was shut down without a finish outcome")]
    SessionClosed,
    /// Scientific sketch configuration or accumulation failed.
    #[error(transparent)]
    Sketch(#[from] SketchError),
    /// Scientific configuration, compatibility, or profiling failed.
    #[error(transparent)]
    Profile(#[from] ProfileError),
    /// A session worker panicked or was cancelled.
    #[error("session task failed: {0}")]
    SessionTask(#[source] JoinError),
}
