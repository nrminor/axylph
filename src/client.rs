use std::{
    io::{self, Cursor, Read},
    sync::Arc,
};

use arrow_array::{
    RecordBatch,
    builder::{ArrayBuilder, BinaryBuilder},
};
use arrow_flight::{
    FlightClient, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_client::FlightServiceClient,
};
use flate2::read::MultiGzDecoder;
use futures_util::StreamExt;
use seq_io::{
    fastq::{Reader, Record},
    policy::DoubleUntilLimited,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

use crate::{
    Error, Limits, Progress,
    protocol::{MESSAGE_BYTES, schema},
};

/// Sends FASTQ reads as Arrow batches and reports receiver validation counts.
pub struct Client {
    endpoint: String,
    limits: Limits,
}

impl Client {
    /// Use an HTTP Flight endpoint and limits for each outgoing batch.
    #[must_use]
    pub const fn new(endpoint: String, limits: Limits) -> Self {
        Self { endpoint, limits }
    }

    /// Read plain or gzip FASTQ and report progress without terminal output.
    ///
    /// The reader runs on a blocking thread. Dropping this future signals
    /// cancellation to the request producer and parser, including a parser
    /// waiting for batch-channel capacity. An executing `Read::read` cannot be
    /// interrupted; use local files or readers with their own timeouts. Once
    /// cancellation is observed after that read returns, its bytes are discarded
    /// and no subsequent source read is started.
    ///
    /// Data already handed to the transport may still reach the receiver, and
    /// accepted reads are not rolled back. The peer may observe request EOF
    /// rather than a cancellation status. No automatic retry is attempted.
    ///
    /// # Errors
    /// Returns an error for invalid input, exceeded limits, connection failure,
    /// or an incomplete upload. Earlier progress is not rolled back on failure.
    pub async fn send<R, F>(&self, reader: R, mut on_progress: F) -> Result<Progress, Error>
    where
        R: Read + Send + 'static,
        F: FnMut(Progress) + Send,
    {
        let channel = Endpoint::from_shared(self.endpoint.clone())?
            .connect()
            .await?;
        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES);
        let mut client = FlightClient::new_from_inner(inner);
        let (sender, receiver) = mpsc::channel(1);
        let (error_sender, mut local_error) = oneshot::channel();
        // No values are sent: dropping the sole sender cancels both producers.
        let (_cancel_on_drop, mut cancelled) = watch::channel(());
        let mut request_cancelled = cancelled.clone();
        let limits = self.limits;
        let parser = tokio::task::spawn_blocking(move || {
            match read_batches(reader, limits, &sender, &mut cancelled) {
                Ok(progress) => Some(progress),
                Err(error) => {
                    let notice = FlightError::protocol(error.to_string());
                    // Preserve the cause before Flight can report the stream failure.
                    let _ = error_sender.send(error);
                    let _ = send_batch(&sender, &mut cancelled, Err(notice));
                    None
                }
            }
        });
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema())
            .with_max_flight_data_size(MESSAGE_BYTES)
            .build(ReceiverStream::new(receiver))
            .take_until(async move {
                let _ = request_cancelled.changed().await;
            });
        let mut responses = client
            .do_put(encoded)
            .await
            .map_err(|error| transfer_error(error, &mut local_error))?;
        let mut last = Progress::default();
        while let Some(response) = responses.next().await {
            let response = response.map_err(|error| transfer_error(error, &mut local_error))?;
            let progress: Progress = serde_json::from_slice(&response.app_metadata)?;
            if last.complete
                || progress.reads < last.reads
                || progress.bases < last.bases
                || (progress.complete
                    && (progress.reads != last.reads || progress.bases != last.bases))
            {
                return Err(Error::InconsistentProgress);
            }
            last = progress;
            if !progress.complete {
                on_progress(progress);
            }
        }
        drop(responses);
        let Some(expected) = parser.await? else {
            return Err(local_error.await.map_err(|_| Error::UploadClosed)?);
        };
        if !last.complete {
            return Err(Error::MissingCompletion);
        }
        if last != expected {
            return Err(Error::CountMismatch {
                expected,
                received: last,
            });
        }
        on_progress(last);
        Ok(last)
    }
}

fn transfer_error(remote: FlightError, local: &mut oneshot::Receiver<Error>) -> Error {
    // A remote failure must not wait for a reader blocked inside Read::read.
    // UploadClosed can be a consequence of that failure, rather than its cause.
    match local.try_recv() {
        Ok(error) if !matches!(error, Error::UploadClosed) => error,
        _ => Error::Flight(remote),
    }
}

fn read_batches<R: Read>(
    reader: R,
    limits: Limits,
    sender: &mpsc::Sender<Result<RecordBatch, FlightError>>,
    cancelled: &mut watch::Receiver<()>,
) -> Result<Progress, Error> {
    let mut reader = CancellableReader {
        reader,
        cancelled: cancelled.clone(),
    };
    let mut prefix = Vec::with_capacity(2);
    reader.by_ref().take(2).read_to_end(&mut prefix)?;
    let gzip = prefix.starts_with(&[0x1f, 0x8b]);
    let input = Cursor::new(prefix).chain(reader);
    let input: Box<dyn Read> = if gzip {
        Box::new(MultiGzDecoder::new(input))
    } else {
        Box::new(input)
    };
    let capacity = limits
        .batch_bytes
        .checked_add(4096)
        .and_then(usize::checked_next_power_of_two)
        .ok_or(Error::Overflow("parser capacity"))?;
    let mut parser = Reader::with_capacity(input, capacity.min(8192))
        .set_policy(DoubleUntilLimited::new(capacity, capacity));
    let mut batch = BatchBuilder::new(limits);
    let mut parsed_reads = 0u64;
    let mut parsed_bases = 0u64;
    loop {
        if cancelled.is_closed() || sender.is_closed() {
            return Err(Error::UploadClosed);
        }
        let record = parser.next();
        if cancelled.is_closed() || sender.is_closed() {
            return Err(Error::UploadClosed);
        }
        let Some(record) = record else {
            break;
        };
        let record = record?;
        parsed_reads = parsed_reads
            .checked_add(1)
            .ok_or(Error::Overflow("read count"))?;
        parsed_bases = parsed_bases
            .checked_add(
                u64::try_from(record.seq().len()).map_err(|_| Error::Overflow("base count"))?,
            )
            .ok_or(Error::Overflow("base count"))?;
        if let Some(ready) = batch.push(&record)? {
            send_batch(sender, cancelled, Ok(ready))?;
        }
    }
    if !batch.is_empty() {
        send_batch(sender, cancelled, Ok(batch.finish()?))?;
    }
    Ok(Progress {
        reads: parsed_reads,
        bases: parsed_bases,
        complete: true,
    })
}

fn send_batch(
    sender: &mpsc::Sender<Result<RecordBatch, FlightError>>,
    cancelled: &mut watch::Receiver<()>,
    batch: Result<RecordBatch, FlightError>,
) -> Result<(), Error> {
    Handle::current().block_on(async {
        tokio::select! {
            biased;
            _ = cancelled.changed() => Err(Error::UploadClosed),
            result = sender.send(batch) => result.map_err(|_| Error::UploadClosed),
        }
    })
}

struct CancellableReader<R> {
    reader: R,
    cancelled: watch::Receiver<()>,
}

impl<R: Read> Read for CancellableReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancelled.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let result = self.reader.read(buffer);
        if self.cancelled.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        result
    }
}

struct BatchBuilder {
    limits: Limits,
    ids: BinaryBuilder,
    sequences: BinaryBuilder,
    qualities: BinaryBuilder,
    bytes: usize,
}

impl BatchBuilder {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            ids: BinaryBuilder::with_capacity(limits.batch_reads, 0),
            sequences: BinaryBuilder::with_capacity(limits.batch_reads, 0),
            qualities: BinaryBuilder::with_capacity(limits.batch_reads, 0),
            bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    fn push(&mut self, record: &impl Record) -> Result<Option<RecordBatch>, Error> {
        let bytes = record
            .head()
            .len()
            .checked_add(record.seq().len())
            .and_then(|n| n.checked_add(record.qual().len()))
            .ok_or(Error::Overflow("read size"))?;
        if bytes > self.limits.batch_bytes {
            return Err(Error::ReadTooLarge {
                bytes,
                limit: self.limits.batch_bytes,
            });
        }
        let ready = if !self.is_empty()
            && (self.ids.len() >= self.limits.batch_reads
                || self.bytes.saturating_add(bytes) > self.limits.batch_bytes)
        {
            Some(self.finish()?)
        } else {
            None
        };
        self.ids.append_value(record.head());
        self.sequences.append_value(record.seq());
        self.qualities.append_value(record.qual());
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(Error::Overflow("batch size"))?;
        Ok(ready)
    }

    fn finish(&mut self) -> Result<RecordBatch, Error> {
        self.bytes = 0;
        let schema = schema();
        let ids = Arc::new(self.ids.finish());
        let sequences = Arc::new(self.sequences.finish());
        let qualities = Arc::new(self.qualities.finish());
        let columns: Vec<arrow_array::ArrayRef> = vec![ids, sequences, qualities];
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}
