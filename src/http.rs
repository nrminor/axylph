use std::{fmt::Display, sync::Arc};

use axum::{
    Router,
    extract::State,
    http::header,
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use futures_util::StreamExt;
use serde::{Serialize, Serializer, ser::SerializeStruct};
use sylph::{
    profile::{ProfileMode, ProfileOptions, ProfileRow, SequenceEstimate},
    types::AdjustStatus,
};
use tokio::sync::watch;

use crate::{Error, Observation, ProcessedCheckpoint, ProfileSnapshot, SessionState, session};

#[derive(Clone)]
struct Source {
    sample_name: Arc<str>,
    updates: watch::Receiver<Arc<Observation>>,
}

pub fn router(
    flight: Router,
    sample_name: Arc<str>,
    updates: watch::Receiver<Arc<Observation>>,
) -> Router {
    flight.merge(
        Router::new()
            .route("/events", get(events))
            .with_state(Source {
                sample_name,
                updates,
            }),
    )
}

async fn events(State(source): State<Source>) -> impl IntoResponse {
    let stream = async_stream::stream! {
        let mut updates = Box::pin(session::observations(source.updates));
        let mut first = true;
        let mut last_profile = None;
        while let Some(observation) = updates.next().await {
            let checkpoint = observation.snapshot().map(|snapshot| snapshot.checkpoint());
            let status = Status::new(&source.sample_name, &observation);
            // Successful checkpoints identify immutable profiles within this session;
            // finish reuses an exact result. Retain no old rows merely for comparison.
            let event = if first || checkpoint != last_profile {
                Event::default().event("snapshot").json_data(Snapshot {
                    status,
                    profile: observation.snapshot().map(|profile| ProfileData(profile)),
                })
            } else {
                Event::default().event("status").json_data(status)
            };
            first = false;
            last_profile = checkpoint;
            drop(observation);
            yield event;
        }
    };
    (
        [(header::CACHE_CONTROL, "no-store")],
        Sse::new(stream).keep_alive(KeepAlive::default()),
    )
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
