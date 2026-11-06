use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use arrow_array::BinaryArray;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::MessageHeader;
use arrow_schema::Schema;
use futures_util::{Stream, StreamExt};
use sylph::{
    read_sketch::{ReadSketcher, SketchError},
    types::SequencesSketch,
};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status, Streaming};

use crate::{Error, Limits, Progress, protocol};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// One sample's cumulative sketch, accepting one upload at a time.
///
/// Clones share the sketch and upload gate. Upload completion and disconnects
/// do not reset the sketch. State is in memory only and grows with the sample.
#[derive(Clone)]
pub struct Receiver {
    limits: Limits,
    upload: Arc<Semaphore>,
    state: Arc<Mutex<SketchState>>,
}

struct SketchState {
    sketcher: ReadSketcher,
    failure: Option<SketchError>,
}

impl SketchState {
    fn check(&self) -> Result<(), Status> {
        if let Some(error) = &self.failure {
            return Err(Status::failed_precondition(format!(
                "sketch ingestion stopped: {error}"
            )));
        }
        Ok(())
    }

    fn extend(&mut self, sequences: &BinaryArray) -> Result<(), Status> {
        self.check()?;
        // Protocol validation has already rejected every null sequence.
        if let Err(error) = self.sketcher.extend(sequences.iter().flatten()) {
            let status = match &error {
                SketchError::MarkerCountOverflow { .. } | SketchError::ReadCountLimit { .. } => {
                    Status::resource_exhausted(error.to_string())
                }
                SketchError::InvalidParameters { .. } | SketchError::InvalidRead { .. } => {
                    Status::internal(error.to_string())
                }
            };
            // Record this inside the job: its response may already be cancelled.
            self.failure = Some(error);
            return Err(status);
        }
        Ok(())
    }
}

impl Receiver {
    /// Adopt a configured accumulator and limits for each incoming batch.
    ///
    /// All uploads belong to this one sample. Its parameters and identity come
    /// from `sketcher` and cannot be changed by clients.
    #[must_use]
    pub fn new(limits: Limits, sketcher: ReadSketcher) -> Self {
        Self {
            limits,
            upload: Arc::new(Semaphore::new(1)),
            state: Arc::new(Mutex::new(SketchState {
                sketcher,
                failure: None,
            })),
        }
    }

    /// Observe a consistent processed prefix on a blocking thread.
    ///
    /// The callback borrows the sketch without copying its marker map or
    /// resetting duplicate history. It may run between batches of an active
    /// upload; pending batches need not be included. Return an owned result.
    /// Keep the callback short and do not re-enter this receiver: ingestion
    /// waits for it to release the sketch lock.
    /// Dropping the future does not stop a submitted callback; it may still run
    /// and hold the lock after its caller has stopped waiting.
    ///
    /// # Errors
    /// Returns an error if the callback task panics or state has been poisoned.
    /// A scientific overflow still allows inspection of the accepted prefix.
    pub async fn inspect<T: Send + 'static>(
        &self,
        observe: impl FnOnce(&SequencesSketch) -> T + Send + 'static,
    ) -> Result<T, Error> {
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let state = state.lock().map_err(|_| Error::SketchPoisoned)?;
            Ok(observe(state.sketcher.sketch()))
        })
        .await
        .map_err(Error::ReceiverTask)?
    }

    /// Build the router serving the Flight protocol.
    pub fn router(self) -> axum::Router {
        let service = FlightServiceServer::new(self)
            .max_decoding_message_size(protocol::MESSAGE_BYTES)
            .max_encoding_message_size(protocol::MESSAGE_BYTES);
        tonic::service::Routes::new(service).into_axum_router()
    }
}

fn validate_message(
    data: &FlightData,
    seen_schema: &mut bool,
    limits: Limits,
) -> Result<(), Status> {
    if !data.app_metadata.is_empty() || data.flight_descriptor.is_some() {
        return Err(Status::invalid_argument(
            "unexpected Flight metadata or descriptor",
        ));
    }
    let message = arrow_ipc::root_as_message(&data.data_header)
        .map_err(|_| Status::invalid_argument("invalid IPC message"))?;
    match message.header_type() {
        MessageHeader::Schema => {
            if *seen_schema {
                return Err(Status::invalid_argument("multiple schemas"));
            }
            let schema = Schema::try_from(data)
                .map_err(|_| Status::invalid_argument("invalid IPC schema"))?;
            if schema != *protocol::schema() {
                return Err(Status::invalid_argument(
                    "expected the axylph version 0 read schema",
                ));
            }
            *seen_schema = true;
        }
        MessageHeader::RecordBatch => {
            if !*seen_schema {
                return Err(Status::invalid_argument("missing schema before batch"));
            }
            let batch = message
                .header_as_record_batch()
                .ok_or_else(|| Status::invalid_argument("invalid IPC batch"))?;
            if batch.compression().is_some() {
                return Err(Status::invalid_argument("IPC compression is not supported"));
            }
            let rows = usize::try_from(batch.length())
                .map_err(|_| Status::invalid_argument("invalid batch length"))?;
            if rows > limits.batch_reads {
                return Err(Status::resource_exhausted("too many reads in one batch"));
            }
        }
        _ => return Err(Status::invalid_argument("unsupported IPC message")),
    }
    Ok(())
}

#[tonic::async_trait]
impl FlightService for Receiver {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list flights"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get flight info"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll flight info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get schema"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do get"))
    }
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let permit = Arc::new(
            Arc::clone(&self.upload)
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("another upload is active"))?,
        );
        let state = Arc::clone(&self.state);
        let check_permit = Arc::clone(&permit);
        // Check even schema-only uploads, without waiting on a lock in Tokio.
        tokio::task::spawn_blocking(move || {
            let _permit = check_permit;
            state
                .lock()
                .map_err(|_| Status::failed_precondition("sketch state is poisoned"))?
                .check()
        })
        .await
        .map_err(|error| Status::internal(Error::ReceiverTask(error).to_string()))??;
        let state = Arc::clone(&self.state);
        let limits = self.limits;
        let mut input = request.into_inner();
        let validated = async_stream::try_stream! {
            let mut seen_schema = false;
            while let Some(message) = input.next().await {
                let message = message.map_err(FlightError::from)?;
                validate_message(&message, &mut seen_schema, limits).map_err(FlightError::from)?;
                yield message;
            }
            if !seen_schema {
                Err(FlightError::from(Status::invalid_argument("missing schema")))?;
            }
        };
        let output = async_stream::try_stream! {
            let permit = permit;
            let mut batches = FlightRecordBatchStream::new_from_flight_data(validated);
            let mut progress = Progress::default();
            while let Some(batch) = batches.next().await {
                let batch = batch.map_err(|error| match error {
                    FlightError::Tonic(status) => *status,
                    other => Status::invalid_argument(other.to_string()),
                })?;
                let state = Arc::clone(&state);
                // A queued or running batch keeps the gate if this stream is dropped.
                let batch_permit = Arc::clone(&permit);
                progress = tokio::task::spawn_blocking(move || {
                    let _permit = batch_permit;
                    let sequences = progress.record(&batch, limits)?;
                    let mut state = state.lock()
                        .map_err(|_| Status::failed_precondition("sketch state is poisoned"))?;
                    state.extend(sequences)?;
                    drop(state);
                    Ok::<_, Status>(progress)
                }).await.map_err(|error| Status::internal(Error::ReceiverTask(error).to_string()))??;
                let metadata = serde_json::to_vec(&progress)
                    .map_err(|error| Status::internal(error.to_string()))?;
                yield PutResult { app_metadata: metadata.into() };
            }
            progress.complete = true;
            let metadata = serde_json::to_vec(&progress)
                .map_err(|error| Status::internal(error.to_string()))?;
            drop(permit);
            yield PutResult { app_metadata: metadata.into() };
        };
        Ok(Response::new(Box::pin(output)))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list actions"))
    }
}
