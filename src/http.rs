use std::{fmt::Display, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use serde_json::{Map, Value};
use sylph::{
    profile::{ProfileMode, ProfileOptions, ProfileRow, SequenceEstimate},
    types::AdjustStatus,
};
use tokio::{sync::watch, time::Instant};

use crate::{
    Error, Observation, ProcessedCheckpoint, ProfileSnapshot, RefreshFailure, SessionState,
    session::FinishHandle,
};

#[derive(Clone)]
struct Source {
    sample_name: Arc<str>,
    updates: watch::Receiver<Arc<Observation>>,
}

#[derive(Clone)]
struct FinishEndpoint {
    observations: Source,
    operation: FinishHandle,
}

#[derive(Serialize)]
struct FinishError {
    error: Failure,
}

async fn finish(
    State(endpoint): State<FinishEndpoint>,
    request: Result<Json<Map<String, Value>>, JsonRejection>,
) -> impl IntoResponse {
    let response = match request {
        Ok(Json(request)) if request.is_empty() => match endpoint.operation.finish().await {
            Ok(_) => {
                let observation = Arc::clone(&endpoint.observations.updates.borrow());
                let status = Status::new(&endpoint.observations.sample_name, &observation);
                let profile = observation.snapshot().map(|profile| ProfileData(profile));
                let snapshot = Snapshot { status, profile };
                Json(snapshot).into_response()
            }
            Err(error) => {
                let status = match error.as_ref() {
                    Error::SessionClosed => StatusCode::CONFLICT,
                    Error::Profile(_) => StatusCode::UNPROCESSABLE_ENTITY,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let failure = Failure::from(error.as_ref());
                let body = FinishError { error: failure };
                (status, Json(body)).into_response()
            }
        },
        Ok(_) => {
            let failure = Failure {
                code: "invalid_finish_request",
                message: "expected an empty JSON object".to_owned(),
            };
            let body = FinishError { error: failure };
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
        Err(error) => {
            let status = error.status();
            let failure = Failure {
                code: "invalid_finish_request",
                message: error.body_text(),
            };
            let body = FinishError { error: failure };
            (status, Json(body)).into_response()
        }
    };
    ([(header::CACHE_CONTROL, "no-store")], response)
}

pub fn router(
    flight: Router,
    sample_name: Arc<str>,
    updates: watch::Receiver<Arc<Observation>>,
    operation: FinishHandle,
) -> Router {
    let source = Source {
        sample_name,
        updates,
    };
    flight
        .merge(
            Router::new()
                .route("/events", get(events))
                .with_state(source.clone()),
        )
        .merge(
            Router::new()
                .route("/finish", post(finish))
                .with_state(FinishEndpoint {
                    observations: source,
                    operation,
                }),
        )
}

async fn events(State(mut source): State<Source>) -> impl IntoResponse {
    let stream = async_stream::stream! {
        let mut delivered: Option<DeliveredObservation> = None;
        loop {
            let observation = Arc::clone(&source.updates.borrow_and_update());
            let checkpoint = observation.snapshot().map(|snapshot| snapshot.checkpoint());
            let new_profile = delivered.as_ref().is_none_or(|last| last.profile != checkpoint);
            let urgent = new_profile
                || delivered.as_ref().is_some_and(|last| last.metadata_changed(&observation));
            let progress = delivered.as_ref().is_none_or(|last| last.processed != observation.processed());
            let remaining = delivered.as_ref().map_or(Duration::ZERO, DeliveredObservation::remaining);
            if urgent || (progress && remaining.is_zero()) {
                let status = Status::new(&source.sample_name, &observation);
                let event = if new_profile {
                    Event::default().event("snapshot").json_data(Snapshot {
                        status,
                        profile: observation.snapshot().map(|profile| ProfileData(profile)),
                    })
                } else {
                    Event::default().event("status").json_data(status)
                };
                let stopped = observation.state() == SessionState::Stopped;
                delivered = Some(DeliveredObservation::new(&observation));
                drop(observation);
                yield event;
                if stopped { break; }
                continue;
            }
            drop(observation);
            // The watch slot is the only pending state. Re-read it at the deadline
            // or after a change, so urgent updates can bypass routine progress.
            tokio::select! {
                changed = source.updates.changed() => if changed.is_err() { break; },
                () = tokio::time::sleep(remaining), if progress => {}
            }
        }
    };
    (
        [(header::CACHE_CONTROL, "no-store")],
        Sse::new(stream).keep_alive(KeepAlive::default()),
    )
}

// Retain metadata, not old scientific rows, across backpressure and progress waits.
struct DeliveredObservation {
    processed: ProcessedCheckpoint,
    profile: Option<ProcessedCheckpoint>,
    state: SessionState,
    ingestion_error: Option<Arc<Error>>,
    refresh_error: Option<Arc<RefreshFailure>>,
    finish: Option<Result<ProcessedCheckpoint, Arc<Error>>>,
    sent_at: Instant,
}

impl DeliveredObservation {
    fn new(observation: &Observation) -> Self {
        Self {
            processed: observation.processed(),
            profile: observation.snapshot().map(|snapshot| snapshot.checkpoint()),
            state: observation.state(),
            ingestion_error: observation.ingestion_error().cloned(),
            refresh_error: observation.refresh_error().cloned(),
            finish: observation.finish_outcome().map(|outcome| {
                outcome
                    .as_ref()
                    .map(|snapshot| snapshot.checkpoint())
                    .map_err(Arc::clone)
            }),
            sent_at: Instant::now(),
        }
    }

    fn metadata_changed(&self, observation: &Observation) -> bool {
        self.state != observation.state()
            || !same_failure(self.ingestion_error.as_ref(), observation.ingestion_error())
            || !same_failure(self.refresh_error.as_ref(), observation.refresh_error())
            || match (&self.finish, observation.finish_outcome()) {
                (None, None) => false,
                (Some(Ok(previous)), Some(Ok(current))) => *previous != current.checkpoint(),
                (Some(Err(previous)), Some(Err(current))) => !Arc::ptr_eq(previous, current),
                _ => true,
            }
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(250).saturating_sub(self.sent_at.elapsed())
    }
}

fn same_failure<T>(previous: Option<&Arc<T>>, current: Option<&Arc<T>>) -> bool {
    match (previous, current) {
        (None, None) => true,
        (Some(previous), Some(current)) => Arc::ptr_eq(previous, current),
        _ => false,
    }
}

// Borrow rows and strings until serialization finishes; only the SSE payload is owned.
#[derive(Serialize)]
struct Snapshot<'a> {
    status: Status<'a>,
    profile: Option<ProfileData<'a>>,
}

#[derive(Serialize)]
struct Status<'a> {
    sample_name: &'a str,
    state: &'static str,
    processed: Checkpoint,
    profile_checkpoint: Option<Checkpoint>,
    ingestion_error: Option<Failure>,
    refresh_error: Option<RefreshError>,
    finish: Option<Finish>,
}

impl<'a> Status<'a> {
    fn new(sample_name: &'a str, observation: &Observation) -> Self {
        Self {
            sample_name,
            state: match observation.state() {
                SessionState::Open => "open",
                SessionState::Stopping => "stopping",
                SessionState::Stopped => "stopped",
            },
            processed: observation.processed().into(),
            profile_checkpoint: observation
                .snapshot()
                .map(|snapshot| snapshot.checkpoint().into()),
            ingestion_error: observation
                .ingestion_error()
                .map(|error| Failure::from(error.as_ref())),
            refresh_error: observation.refresh_error().map(|failure| RefreshError {
                checkpoint: failure.checkpoint().into(),
                error: Failure {
                    code: "profile",
                    message: failure.error().to_string(),
                },
            }),
            finish: observation.finish_outcome().map(|outcome| match outcome {
                Ok(snapshot) => Finish::Succeeded {
                    checkpoint: snapshot.checkpoint().into(),
                },
                Err(error) => Finish::Failed {
                    error: Failure::from(error.as_ref()),
                },
            }),
        }
    }
}

#[derive(Serialize)]
struct Failure {
    code: &'static str,
    message: String,
}

impl From<&Error> for Failure {
    fn from(error: &Error) -> Self {
        Self {
            code: match error {
                Error::Profile(_) => "profile",
                Error::Sketch(_) => "sketch",
                Error::Overflow(_) => "overflow",
                Error::SketchPoisoned => "sketch_poisoned",
                Error::ReceiverTask(_) => "receiver_task",
                Error::SessionTask(_) => "session_task",
                Error::SessionClosed => "session_closed",
                _ => "session",
            },
            message: error.to_string(),
        }
    }
}

#[derive(Serialize)]
struct RefreshError {
    checkpoint: Checkpoint,
    error: Failure,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Finish {
    Succeeded { checkpoint: Checkpoint },
    Failed { error: Failure },
}

struct Count<T>(T);

impl<T: Display> Serialize for Count<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

#[derive(Serialize)]
struct Checkpoint {
    reads: Count<u64>,
    bases: Count<u64>,
}

impl From<ProcessedCheckpoint> for Checkpoint {
    fn from(checkpoint: ProcessedCheckpoint) -> Self {
        Self {
            reads: Count(checkpoint.reads()),
            bases: Count(checkpoint.bases()),
        }
    }
}

struct ProfileData<'a>(&'a ProfileSnapshot);

impl Serialize for ProfileData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let profile = self.0.profile();
        let mut data = serializer.serialize_struct("Profile", 4)?;
        data.serialize_field("checkpoint", &Checkpoint::from(self.0.checkpoint()))?;
        data.serialize_field("sample_name", &profile.sample_name)?;
        data.serialize_field("options", &Options::from(profile.options))?;
        data.serialize_field("rows", &Rows(&profile.rows))?;
        data.end()
    }
}

#[derive(Serialize)]
struct Options {
    minimum_ani_percent: f64,
    min_contain: usize,
    min_number_kmers: f64,
    confidence_intervals: bool,
    mode: &'static str,
    read_identity_percent: Option<f64>,
}

impl From<ProfileOptions> for Options {
    fn from(options: ProfileOptions) -> Self {
        Self {
            minimum_ani_percent: options.minimum_ani,
            min_contain: options.min_contain,
            min_number_kmers: options.min_number_kmers,
            confidence_intervals: options.confidence_intervals,
            mode: match options.mode {
                ProfileMode::RelativeAbundance => "relative_abundance",
                ProfileMode::EstimateUnknown => "estimate_unknown",
                ProfileMode::EstimateReadCounts => "estimate_read_counts",
            },
            read_identity_percent: options.read_identity,
        }
    }
}

struct Rows<'a>(&'a [ProfileRow]);

impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(Row))
    }
}

struct Row<'a>(&'a ProfileRow);

impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let row = self.0;
        let mut data = serializer.serialize_struct("ProfileRow", 14)?;
        data.serialize_field("genome_name", &row.genome_name)?;
        data.serialize_field("contig_name", &row.contig_name)?;
        data.serialize_field("naive_ani_fraction", &row.naive_ani)?;
        data.serialize_field("adjusted_ani_fraction", &row.adjusted_ani)?;
        data.serialize_field("effective_coverage", &row.effective_coverage)?;
        data.serialize_field("mean_coverage", &row.mean_coverage)?;
        data.serialize_field("median_coverage", &row.median_coverage)?;
        data.serialize_field(
            "containment",
            &Containment {
                contained: Count(row.containment.0),
                sampled: Count(row.containment.1),
            },
        )?;
        data.serialize_field(
            "adjustment",
            &match &row.adjustment {
                AdjustStatus::Lambda(value) => Estimate::Lambda(*value),
                AdjustStatus::Low => Estimate::Low,
                AdjustStatus::High => Estimate::High,
            },
        )?;
        data.serialize_field("ani_interval_fraction", &row.ani_interval)?;
        data.serialize_field("lambda_interval", &row.lambda_interval)?;
        data.serialize_field(
            "taxonomic_abundance_percent",
            &row.taxonomic_abundance_percent,
        )?;
        data.serialize_field(
            "sequence_estimate",
            &match row.sequence_estimate {
                SequenceEstimate::Percent(value) => Estimate::Percent(value),
                SequenceEstimate::EstimatedReadCount(value) => Estimate::EstimatedReadCount(value),
            },
        )?;
        data.serialize_field("kmers_reassigned", &Count(row.kmers_reassigned))?;
        data.end()
    }
}

#[derive(Serialize)]
struct Containment {
    contained: Count<usize>,
    sampled: Count<usize>,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum Estimate {
    Lambda(f64),
    Low,
    High,
    Percent(f64),
    EstimatedReadCount(f64),
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use axum::{body::BodyDataStream, extract::State, response::IntoResponse};
    use futures_util::{FutureExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::{sync::watch, time};

    use super::{Source, events};
    use crate::{Observation, ProcessedCheckpoint, SessionState};

    fn progress(reads: u64, bases: u64) -> Arc<Observation> {
        let mut observation = Observation::default();
        observation.processed = ProcessedCheckpoint { reads, bases };
        Arc::new(observation)
    }

    // Parse SSE boundaries independently of Axum's body-frame boundaries.
    async fn next_event(
        body: &mut BodyDataStream,
        buffered: &mut String,
    ) -> Result<(String, Value), Box<dyn Error>> {
        for frames_read in 0..=16 {
            if let Some(end) = buffered.find("\n\n") {
                let event: String = buffered.drain(..end.saturating_add(2)).collect();
                let name = event
                    .lines()
                    .find_map(|line| line.strip_prefix("event: "))
                    .ok_or("SSE event has no event name")?;
                let data = event
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .collect::<Vec<_>>()
                    .join("\n");
                return Ok((name.to_owned(), serde_json::from_str(&data)?));
            }
            if frames_read == 16 {
                break;
            }
            let bytes = body
                .next()
                .await
                .ok_or("SSE body ended before a complete event")??;
            buffered.push_str(std::str::from_utf8(&bytes)?);
        }
        Err("SSE event exceeded the test's 16-frame bound".into())
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn progress_events_are_coalesced_until_the_next_250ms_deadline() {
        // A controlled source isolates stream cadence from profiling and networking.
        let (updates, receiver) = watch::channel(progress(0, 0));
        let response = events(State(Source {
            sample_name: Arc::from("cadence"),
            updates: receiver,
        }))
        .await
        .into_response();
        let mut body = response.into_body().into_data_stream();
        let mut buffered = String::new();
        let started = time::Instant::now();
        let (name, initial) =
            time::timeout(Duration::from_secs(1), next_event(&mut body, &mut buffered))
                .await
                .expect("initial snapshot must arrive within one virtual second")
                .expect("initial snapshot must be a complete JSON SSE event");
        assert_eq!(name, "snapshot");
        assert_eq!(
            initial["status"]["processed"],
            json!({"reads": "0", "bases": "0"})
        );
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "initial snapshot must be immediate"
        );

        updates
            .send(progress(1, 510))
            .expect("SSE receiver must remain subscribed");
        time::advance(Duration::from_millis(250)).await;
        let (name, first) =
            time::timeout(Duration::from_secs(1), next_event(&mut body, &mut buffered))
                .await
                .expect("first status must arrive within one virtual second")
                .expect("first status must be a complete JSON SSE event");
        assert_eq!(name, "status");
        assert_eq!(first["processed"], json!({"reads": "1", "bases": "510"}));
        assert_eq!(started.elapsed(), Duration::from_millis(250));

        updates
            .send(progress(2, 1020))
            .expect("SSE receiver must remain subscribed");
        let mut next = Box::pin(next_event(&mut body, &mut buffered));
        assert!(
            next.as_mut().now_or_never().is_none(),
            "progress 2 emitted at the previous event's timestamp; throttle routine status events"
        );
        updates
            .send(progress(3, 1530))
            .expect("SSE receiver must remain subscribed");
        assert!(
            next.as_mut().now_or_never().is_none(),
            "progress 3 must replace pending progress, not bypass the cadence"
        );
        time::advance(Duration::from_millis(249)).await;
        assert!(
            next.as_mut().now_or_never().is_none(),
            "routine progress must remain pending until the full 250ms deadline"
        );
        time::advance(Duration::from_millis(1)).await;
        let (name, latest) = time::timeout(Duration::from_secs(1), next)
            .await
            .expect("coalesced status must arrive within one virtual second")
            .expect("coalesced status must be a complete JSON SSE event");
        assert_eq!(name, "status");
        assert_eq!(
            latest["processed"],
            json!({"reads": "3", "bases": "1530"}),
            "the cadence deadline must emit the newest progress, never stale progress 2"
        );
        assert_eq!(started.elapsed(), Duration::from_millis(500));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn stopping_bypasses_pending_progress_cadence_with_newest_counts() {
        let (updates, receiver) = watch::channel(progress(0, 0));
        let response = events(State(Source {
            sample_name: Arc::from("cadence"),
            updates: receiver,
        }))
        .await
        .into_response();
        let mut body = response.into_body().into_data_stream();
        let mut buffered = String::new();
        let started = time::Instant::now();
        let (name, initial) =
            time::timeout(Duration::from_secs(1), next_event(&mut body, &mut buffered))
                .await
                .expect("initial snapshot must arrive within one virtual second")
                .expect("initial snapshot must be a complete JSON SSE event");
        assert_eq!(name, "snapshot");
        assert_eq!(initial["status"]["state"], "open");
        assert_eq!(started.elapsed(), Duration::ZERO);

        updates
            .send(progress(1, 510))
            .expect("SSE receiver must remain subscribed");
        let mut next = Box::pin(next_event(&mut body, &mut buffered));
        assert!(
            next.as_mut().now_or_never().is_none(),
            "routine progress must remain pending before the cadence deadline"
        );

        let mut stopping = progress(2, 1020);
        Arc::make_mut(&mut stopping).state = SessionState::Stopping;
        updates
            .send(stopping)
            .expect("SSE receiver must remain subscribed for the stopping transition");
        let (name, status) = next
            .as_mut()
            .now_or_never()
            .expect(
                "stopping must immediately unblock the pending SSE event without advancing time",
            )
            .expect("stopping status must be a complete JSON SSE event");
        assert_eq!(name, "status");
        assert_eq!(status["state"], "stopping");
        assert_eq!(
            status["processed"],
            json!({"reads": "2", "bases": "1020"}),
            "stopping must carry newest counts rather than the pending routine progress"
        );
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "stopping must bypass the routine progress cadence"
        );
    }
}
