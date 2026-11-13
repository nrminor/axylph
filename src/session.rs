use std::{sync::Arc, time::Duration};

use futures_util::{
    FutureExt, Stream,
    future::{BoxFuture, Shared},
};
use sylph::{
    profile::{Profile, ProfileError, Profiler},
    read_sketch::{Deduplication, ReadSketcher, SketchParams},
};
use tokio::{runtime::Handle, sync::watch, time::Instant};

use crate::{
    Error, Limits, Receiver,
    receiver::{CloseReason, Ingestion},
};

/// Server-authoritative totals for a completely processed read prefix.
/// Counts include duplicates and reads contributing no markers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessedCheckpoint {
    pub(crate) reads: u64,
    pub(crate) bases: u64,
}

impl ProcessedCheckpoint {
    /// Number of completely processed reads.
    #[must_use]
    pub const fn reads(self) -> u64 {
        self.reads
    }

    /// Number of bases in those complete reads.
    #[must_use]
    pub const fn bases(self) -> u64 {
        self.bases
    }
}

/// A successful cumulative calculation and the evidence it covers.
#[derive(Debug)]
pub struct ProfileSnapshot {
    checkpoint: ProcessedCheckpoint,
    profile: Profile,
}

impl ProfileSnapshot {
    /// Processed evidence captured with the calculation's scientific input.
    #[must_use]
    pub const fn checkpoint(&self) -> ProcessedCheckpoint {
        self.checkpoint
    }

    /// Sylph's scientific results, without cloning their rows.
    #[must_use]
    pub const fn profile(&self) -> &Profile {
        &self.profile
    }
}

/// A failed periodic calculation, not a successful empty result.
#[derive(Debug)]
pub struct RefreshFailure {
    checkpoint: ProcessedCheckpoint,
    error: ProfileError,
}

impl RefreshFailure {
    /// Evidence supplied to the failed calculation.
    #[must_use]
    pub const fn checkpoint(&self) -> ProcessedCheckpoint {
        self.checkpoint
    }

    /// The scientific failure. Older successful results remain available.
    #[must_use]
    pub const fn error(&self) -> &ProfileError {
        &self.error
    }
}

/// Worker lifecycle. Finality is reported separately by [`Observation::finish_outcome`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionState {
    /// Periodic profiling is active; an ingestion error may independently block reads.
    #[default]
    Open,
    /// Admission is closed; already admitted work is draining.
    Stopping,
    /// Admitted work has stopped. A recorded successful finish establishes finality.
    Stopped,
}

/// Immutable current session state, cheaply shared with observers.
/// Current progress can be ahead of the latest successful profile's checkpoint.
#[derive(Clone, Debug, Default)]
pub struct Observation {
    pub(crate) processed: ProcessedCheckpoint,
    pub(crate) snapshot: Option<Arc<ProfileSnapshot>>,
    pub(crate) state: SessionState,
    pub(crate) ingestion_error: Option<Arc<Error>>,
    pub(crate) refresh_error: Option<Arc<RefreshFailure>>,
    finish_outcome: Option<Result<Arc<ProfileSnapshot>, Arc<Error>>>,
}

impl Observation {
    /// Current server-authoritative processed totals, not receipt totals.
    #[must_use]
    pub const fn processed(&self) -> ProcessedCheckpoint {
        self.processed
    }

    /// Latest successful profile, if any. Sharing it does not copy scientific rows.
    #[must_use]
    pub const fn snapshot(&self) -> Option<&Arc<ProfileSnapshot>> {
        self.snapshot.as_ref()
    }

    /// Whether admission is open, draining, or stopped.
    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// A failure that stopped scientific ingestion or the session worker.
    #[must_use]
    pub const fn ingestion_error(&self) -> Option<&Arc<Error>> {
        self.ingestion_error.as_ref()
    }

    /// Latest failed refresh, cleared by a later successful calculation.
    #[must_use]
    pub const fn refresh_error(&self) -> Option<&Arc<RefreshFailure>> {
        self.refresh_error.as_ref()
    }

    /// Completed finish result, retained after shutdown. `None` means no completed
    /// finish, not a successful empty profile. Only `Some(Ok(...))` establishes finality.
    /// An ingestion error remains independently visible, even for a final profile.
    #[must_use]
    pub const fn finish_outcome(&self) -> Option<&Result<Arc<ProfileSnapshot>, Arc<Error>>> {
        self.finish_outcome.as_ref()
    }
}

/// Owns one sample's fresh accumulator, periodic profiling, and worker lifetime.
///
/// Call [`Self::shutdown`] to close admission and await admitted work. Dropping
/// the owner requests stopping but cannot await or interrupt synchronous work.
/// Upload EOF does not stop the session. [`Self::finish`] explicitly requests a
/// profile of the terminal processed prefix; shutdown alone does not finalize it.
pub struct Session {
    receiver: Receiver,
    sample_name: Arc<str>,
    worker: Shared<BoxFuture<'static, Result<(), Arc<Error>>>>,
}

impl Session {
    /// Configure a fresh sample with sylph's defaults and one-second capture-start spacing.
    /// The supplied profiler already owns its references, options, and worker pool.
    pub fn builder(sample_name: impl Into<String>, profiler: Profiler) -> SessionBuilder {
        SessionBuilder {
            sample_name: sample_name.into(),
            profiler,
            limits: Limits::default(),
            sketch_params: SketchParams::default(),
            refresh_interval: Duration::from_secs(1),
        }
    }

    /// Start periodic profiling with fully explicit settings on the caller's Tokio runtime.
    /// No reads are admitted until all configuration and compatibility checks pass.
    /// The runtime must have its time driver enabled.
    ///
    /// # Errors
    /// Rejects invalid/incompatible sketch settings, an invalid interval, or no runtime.
    pub fn start(
        limits: Limits,
        sample_name: impl Into<String>,
        sketch_params: SketchParams,
        profiler: Profiler,
        refresh_interval: Duration,
    ) -> Result<Self, Error> {
        if refresh_interval.is_zero() || Instant::now().checked_add(refresh_interval).is_none() {
            return Err(Error::InvalidRefreshInterval);
        }
        let sample_name = sample_name.into();
        let sketcher = ReadSketcher::new(
            sketch_params,
            "axylph".to_owned(),
            Some(sample_name.clone()),
        )?;
        profiler.validate_sample_parameters(sketch_params.c, sketch_params.k)?;
        let runtime = Handle::try_current().map_err(|_| Error::MissingRuntime)?;
        let receiver = Receiver::new(limits, sketcher);
        let ingestion = Arc::clone(&receiver.ingestion);
        let task = runtime.spawn(async move {
            let profiler = Arc::new(profiler);
            let mut result = refresh(&ingestion, &profiler, refresh_interval)
                .await
                .map_err(Arc::new);
            if result.is_err() {
                ingestion.close(CloseReason::Shutdown);
            }
            ingestion.drain().await;
            let finishing = *ingestion.closed.borrow() == Some(CloseReason::Finish);
            if finishing {
                let outcome = match &result {
                    Ok(()) => terminal_profile(Arc::clone(&ingestion), profiler)
                        .await
                        .map_err(Arc::new),
                    Err(error) => Err(Arc::clone(error)),
                };
                if let Err(error) = &outcome
                    && !matches!(error.as_ref(), Error::Profile(_))
                {
                    result = Err(Arc::clone(error));
                }
                ingestion.observations.send_modify(|observation| {
                    let observation = Arc::make_mut(observation);
                    if let Ok(snapshot) = &outcome {
                        observation.snapshot = Some(Arc::clone(snapshot));
                        observation.refresh_error = None;
                    }
                    observation.finish_outcome = Some(outcome);
                });
            }
            if let Err(error) = &result {
                ingestion.observations.send_modify(|observation| {
                    Arc::make_mut(observation).ingestion_error = Some(Arc::clone(error));
                });
            }
            ingestion.observations.send_modify(|observation| {
                Arc::make_mut(observation).state = SessionState::Stopped;
            });
            result
        });
        let worker = async move {
            task.await
                .map_err(|error| Arc::new(Error::SessionTask(error)))?
        }
        .boxed()
        .shared();
        Ok(Self {
            receiver,
            sample_name: sample_name.into(),
            worker,
        })
    }

    /// Obtain a Flight adapter sharing this session's accumulator and admission gate.
    #[must_use]
    pub fn receiver(&self) -> Receiver {
        self.receiver.clone()
    }

    /// Serve Flight uploads and read-only server-sent observations at `GET /events`.
    /// Each subscription starts with a complete snapshot, then receives coalesced
    /// snapshot or status replacements. Reconnection resynchronizes without replay.
    /// Retaining this router does not retain the session owner: dropping the owner
    /// still requests shutdown. A client disconnect does not stop the session.
    /// Authentication, TLS, and cross-origin access belong to the host application.
    pub fn router(&self) -> axum::Router {
        crate::http::router(
            self.receiver().router(),
            Arc::clone(&self.sample_name),
            self.receiver.ingestion.observations.subscribe(),
        )
    }

    /// Subscribe to current state and then coalesced changes until the worker stops.
    /// The owned stream does not keep the session owner alive. Slow observers may skip
    /// intermediate states; retaining old observations retains their shared profiles.
    pub fn observe(&self) -> impl Stream<Item = Arc<Observation>> + Send + use<> {
        observations(self.receiver.ingestion.observations.subscribe())
    }

    /// Close admission and return a successful profile covering the terminal checkpoint.
    /// Reuses an exact successful snapshot or performs one required terminal calculation.
    /// Repeated calls observe the same outcome, including failure; they do not retry.
    /// Cancelling the wait does not cancel an accepted finish request.
    ///
    /// # Errors
    /// Rejects finish after shutdown won admission closure. A failed calculation leaves
    /// admission closed, preserves any older profile as non-final, and records the failure.
    pub async fn finish(&self) -> Result<Arc<ProfileSnapshot>, Arc<Error>> {
        if self.receiver.ingestion.close(CloseReason::Finish) != CloseReason::Finish {
            return Err(Arc::new(Error::SessionClosed));
        }
        self.worker.clone().await?;
        self.receiver
            .ingestion
            .observations
            .borrow()
            .finish_outcome
            .clone()
            .ok_or_else(|| Arc::new(Error::SessionClosed))?
    }

    /// Close admission, stop periodic scheduling, and await admitted work.
    /// Does not wait for client EOF. Repeated/concurrent calls await the same worker.
    /// Cancelling this wait does not cancel draining or lose ownership of the worker.
    /// Does not request finalization, but honors an already accepted finish request.
    /// Scientific finish failures remain available through [`Observation::finish_outcome`].
    ///
    /// # Errors
    /// Returns a shared task/state failure if the worker could not complete normally.
    pub async fn shutdown(&self) -> Result<(), Arc<Error>> {
        self.receiver.ingestion.close(CloseReason::Shutdown);
        self.worker.clone().await
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.receiver.ingestion.close(CloseReason::Shutdown);
    }
}

pub fn observations(
    mut updates: watch::Receiver<Arc<Observation>>,
) -> impl Stream<Item = Arc<Observation>> + Send {
    async_stream::stream! {
        loop {
            let observation = Arc::clone(&updates.borrow_and_update());
            let stopped = observation.state == SessionState::Stopped;
            yield observation;
            if stopped || updates.changed().await.is_err() { break; }
        }
    }
}

/// Configuration only: no session work starts until [`Self::start`].
/// Whole-value and individual setters modify one configuration; later calls win.
#[must_use]
pub struct SessionBuilder {
    sample_name: String,
    profiler: Profiler,
    limits: Limits,
    sketch_params: SketchParams,
    refresh_interval: Duration,
}

impl SessionBuilder {
    /// Replace decoded per-batch limits (default [`Limits::default`]).
    pub const fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Replace all scientific sketch parameters.
    pub const fn sketch_params(mut self, params: SketchParams) -> Self {
        self.sketch_params = params;
        self
    }
    /// Override sampling factor (default 200); must not exceed the minimum reference c.
    pub const fn c(mut self, c: usize) -> Self {
        self.sketch_params.c = c;
        self
    }
    /// Override k-mer length (default 31); must match the reference k.
    pub const fn k(mut self, k: usize) -> Self {
        self.sketch_params.k = k;
        self
    }
    /// Override single-end duplicate policy (default exact).
    pub const fn deduplication(mut self, policy: Deduplication) -> Self {
        self.sketch_params.deduplication = policy;
        self
    }
    /// Set minimum spacing between periodic input-capture starts (default one second).
    /// Long calculations can run back-to-back; this is not a completion cooldown
    /// or deadline. Idle input checks use the same interval. Finish bypasses this wait.
    pub const fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }
    /// Validate the final configuration and start a fresh session.
    ///
    /// # Errors
    /// Returns the same configuration/runtime errors as [`Session::start`].
    pub fn start(self) -> Result<Session, Error> {
        Session::start(
            self.limits,
            self.sample_name,
            self.sketch_params,
            self.profiler,
            self.refresh_interval,
        )
    }
}

struct RefreshCadence {
    interval: Duration,
    eligible_at: Instant,
}

impl RefreshCadence {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            eligible_at: Instant::now(),
        }
    }

    async fn wait(&self) {
        tokio::time::sleep_until(self.eligible_at).await;
    }

    fn after_attempt(&mut self, started: Option<Instant>) -> Result<(), Error> {
        // No-input checks still need a future wake-up, not a busy loop. Actual
        // attempts use capture start on success and failure, never completion.
        let anchor = started.unwrap_or_else(Instant::now);
        self.eligible_at = anchor
            .checked_add(self.interval)
            .ok_or(Error::InvalidRefreshInterval)?;
        Ok(())
    }
}

async fn refresh(
    ingestion: &Arc<Ingestion>,
    profiler: &Arc<Profiler>,
    interval: Duration,
) -> Result<(), Error> {
    let mut closed = ingestion.closed.subscribe();
    let mut cadence = RefreshCadence::new(interval);
    let mut attempted = ProcessedCheckpoint::default();
    loop {
        if closed.borrow().is_some() {
            break;
        }
        tokio::select! {
            biased;
            _ = closed.changed() => break,
            () = cadence.wait() => {}
        }
        let input = Arc::clone(ingestion);
        let profiler = Arc::clone(profiler);
        let calculated = tokio::task::spawn_blocking(move || {
            let Some(capture) = input.capture(attempted)? else {
                return Ok(None);
            };
            let checkpoint = capture.checkpoint;
            let started_at = capture.started_at;
            let result = profiler.profile(&capture.sketch);
            Ok::<_, Error>(Some((checkpoint, started_at, result)))
        })
        .await
        .map_err(Error::SessionTask)??;
        let Some((checkpoint, started_at, result)) = calculated else {
            cadence.after_attempt(None)?;
            continue;
        };
        cadence.after_attempt(Some(started_at))?;
        attempted = checkpoint;
        ingestion.observations.send_modify(|observation| {
            let observation = Arc::make_mut(observation);
            match result {
                Ok(profile) => {
                    observation.snapshot = Some(Arc::new(ProfileSnapshot {
                        checkpoint,
                        profile,
                    }));
                    observation.refresh_error = None;
                }
                Err(error) => {
                    observation.refresh_error =
                        Some(Arc::new(RefreshFailure { checkpoint, error }));
                }
            }
        });
    }
    Ok(())
}

async fn terminal_profile(
    ingestion: Arc<Ingestion>,
    profiler: Arc<Profiler>,
) -> Result<Arc<ProfileSnapshot>, Error> {
    tokio::task::spawn_blocking(move || {
        // Admission is closed and admitted ingestion has drained. Check the
        // scientific state even when a published result appears reusable.
        let checkpoint = ingestion.checkpoint()?;
        let snapshot = ingestion.observations.borrow().snapshot.clone();
        if let Some(snapshot) = snapshot.filter(|snapshot| snapshot.checkpoint == checkpoint) {
            return Ok(snapshot);
        }
        let (sketch, checkpoint) = ingestion.capture_terminal()?;
        let profile = profiler.profile(&sketch)?;
        Ok(Arc::new(ProfileSnapshot {
            checkpoint,
            profile,
        }))
    })
    .await
    .map_err(Error::SessionTask)?
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::FutureExt;
    use tokio::time::{Instant, advance};

    use super::RefreshCadence;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn periodic_capture_waits_from_actual_start_without_post_work_cooldown() {
        let origin = Instant::now();
        let mut cadence = RefreshCadence::new(Duration::from_millis(1000));
        assert_eq!(cadence.wait().now_or_never(), Some(()));
        let first_start = Instant::now();

        // Work longer than the interval must not introduce a completion-based cooldown.
        advance(Duration::from_millis(1500)).await;
        cadence
            .after_attempt(Some(first_start))
            .expect("first deadline must fit the clock; inspect capture time and interval");
        assert_eq!(cadence.wait().now_or_never(), Some(()));
        let second_start = Instant::now();
        assert_eq!(
            second_start.duration_since(origin),
            Duration::from_millis(1500)
        );

        advance(Duration::from_millis(100)).await;
        cadence
            .after_attempt(Some(second_start))
            .expect("second deadline must fit the clock; inspect capture time and interval");
        {
            let wait = cadence.wait();
            tokio::pin!(wait);
            assert_eq!(wait.as_mut().now_or_never(), None);
            advance(Duration::from_millis(400)).await;
            // The old interval grid would incorrectly permit another capture at 2000ms.
            assert_eq!(wait.as_mut().now_or_never(), None);
            advance(Duration::from_millis(499)).await;
            assert_eq!(wait.as_mut().now_or_never(), None);
            advance(Duration::from_millis(1)).await;
            // Eligibility is 2500ms, not the completion-based deadline of 2600ms.
            assert_eq!(wait.as_mut().now_or_never(), Some(()));
        }
        assert_eq!(origin.elapsed(), Duration::from_millis(2500));

        cadence
            .after_attempt(None)
            .expect("an idle check must schedule another poll rather than busy-spin");
        let wait = cadence.wait();
        tokio::pin!(wait);
        assert_eq!(wait.as_mut().now_or_never(), None);
        advance(Duration::from_millis(999)).await;
        assert_eq!(wait.as_mut().now_or_never(), None);
        advance(Duration::from_millis(1)).await;
        assert_eq!(wait.as_mut().now_or_never(), Some(()));
        assert_eq!(origin.elapsed(), Duration::from_millis(3500));
    }
}
