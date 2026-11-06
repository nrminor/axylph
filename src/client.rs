use std::io::{Cursor, Read};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder};
use arrow_flight::FlightClient;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use flate2::read::MultiGzDecoder;
use futures_util::StreamExt;
use seq_io::fastq::{Reader, Record};
use seq_io::policy::DoubleUntilLimited;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

use crate::protocol::{MESSAGE_BYTES, schema};
use crate::{Error, Limits, Progress};

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
    /// The reader runs on a blocking thread. A stalled reader cannot be forcibly
    /// interrupted; use local files or readers with their own timeouts. Dropping
    /// this future closes the transfer, and parsing stops at the next channel
    /// or record check. No automatic retry is attempted.
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
        let limits = self.limits;
        let parser = tokio::task::spawn_blocking(move || {
            let result = read_batches(reader, limits, &sender);
            if let Err(error) = &result {
                let _ = sender.blocking_send(Err(FlightError::protocol(error.to_string())));
            }
            result
        });
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema())
            .with_max_flight_data_size(MESSAGE_BYTES)
            .build(ReceiverStream::new(receiver));
        let mut responses = client.do_put(encoded).await?;
        let mut last = Progress::default();
        while let Some(response) = responses.next().await {
            let progress: Progress = serde_json::from_slice(&response?.app_metadata)?;
            if last.complete
                || progress.reads < last.reads
                || progress.bases < last.bases
                || (progress.complete
                    && (progress.reads != last.reads || progress.bases != last.bases))
            {
                return Err("receiver returned inconsistent progress".into());
            }
            last = progress;
            if !progress.complete {
                on_progress(progress);
            }
        }
        drop(responses);
        let expected = parser.await??;
        if !last.complete {
            return Err("upload ended without a completion receipt".into());
        }
        if last != expected {
            return Err("receiver counts do not match the input".into());
        }
        on_progress(last);
        Ok(last)
    }
}

fn read_batches<R: Read>(
    reader: R,
    limits: Limits,
    sender: &mpsc::Sender<Result<RecordBatch, FlightError>>,
) -> Result<Progress, Error> {
    let mut reader = reader;
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
        .ok_or("parser capacity overflow")?;
    let mut parser = Reader::with_capacity(input, capacity.min(8192))
        .set_policy(DoubleUntilLimited::new(capacity, capacity));
    let mut batch = BatchBuilder::new(limits);
    let mut parsed_reads = 0u64;
    let mut parsed_bases = 0u64;
    while let Some(record) = parser.next() {
        if sender.is_closed() {
            return Err("receiver closed the upload".into());
        }
        let record = record?;
        parsed_reads = parsed_reads.checked_add(1).ok_or("read count overflow")?;
        parsed_bases = parsed_bases
            .checked_add(u64::try_from(record.seq().len())?)
            .ok_or("base count overflow")?;
        if let Some(ready) = batch.push(&record)? {
            sender.blocking_send(Ok(ready))?;
        }
    }
    if !batch.is_empty() {
        sender.blocking_send(Ok(batch.finish()?))?;
    }
    Ok(Progress {
        reads: parsed_reads,
        bases: parsed_bases,
        complete: true,
    })
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
            .ok_or("read size overflow")?;
        if bytes > self.limits.batch_bytes {
            return Err("one read exceeds the batch byte limit".into());
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
        self.bytes = self.bytes.checked_add(bytes).ok_or("batch size overflow")?;
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
