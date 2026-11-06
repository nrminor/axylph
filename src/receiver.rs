use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::MessageHeader;
use arrow_schema::Schema;
use futures_util::{Stream, StreamExt};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status, Streaming};

use crate::{Limits, Progress, protocol};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// A bounded receiver for validating one upload at a time.
pub struct Receiver {
    limits: Limits,
    upload: Arc<Semaphore>,
}

impl Receiver {
    /// Create a receiver with limits for each batch.
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            upload: Arc::new(Semaphore::new(1)),
        }
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
        let permit = Arc::clone(&self.upload)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("another upload is active"))?;
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
                progress.record(&batch, limits)?;
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
