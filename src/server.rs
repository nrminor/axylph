//! Executable-owned listeners and shutdown lifecycle.

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use axylph::{IngestionStatus, Limits, Observation, Receiver, RefreshFailure, Session};
use color_eyre::eyre::{Result, WrapErr, eyre};
use futures_util::StreamExt;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use sylph::{
    profile::{ProfileOptions, Profiler},
    read_sketch::{ReadSketcher, SketchParams},
};
use tokio::{net::TcpListener, task::JoinSet};

enum Owner {
    Receiver(Receiver),
    Session(Session),
}

#[derive(Default)]
struct Warnings {
    ingestion: Option<Arc<axylph::Error>>,
    refresh: Option<Arc<RefreshFailure>>,
}

impl Warnings {
    fn report(
        &mut self,
        display: &crate::display::Display,
        status: &IngestionStatus,
        observation: Option<&Observation>,
    ) {
        if let Some(error) = &status.ingestion_error
            && !self
                .ingestion
                .as_ref()
                .is_some_and(|previous| Arc::ptr_eq(previous, error))
        {
            display.message(&format!("warning: ingestion failed: {error}"));
            self.ingestion = Some(Arc::clone(error));
        }
        if let Some(observation) = observation {
            if let Some(error) = observation.refresh_error() {
                if !self
                    .refresh
                    .as_ref()
                    .is_some_and(|previous| Arc::ptr_eq(previous, error))
                {
                    display.message(&format!(
                        "warning: profile refresh failed: {}",
                        error.error()
                    ));
                    self.refresh = Some(Arc::clone(error));
                }
            } else {
                self.refresh = None;
            }
        }
    }
}

impl Owner {
    async fn new(settings: &Settings) -> Result<Self> {
        if let Some(profiling) = &settings.profiling {
            let path = profiling.reference.clone();
            let workers = profiling.workers.get();
            let profiler = tokio::task::spawn_blocking(move || {
                Profiler::open_syldb(&path, ProfileOptions::default(), workers)
            })
            .await
            .wrap_err("reference loader task failed")?
            .wrap_err_with(|| {
                format!("could not load reference {}", profiling.reference.display())
            })?;
            let session = Session::builder(&settings.sample, profiler)
                .limits(settings.limits)
                .sketch_params(settings.params)
                .refresh_interval(profiling.interval)
                .start()?;
            Ok(Self::Session(session))
        } else {
            let sketcher = ReadSketcher::new(
                settings.params,
                "axylph".to_owned(),
                Some(settings.sample.clone()),
            )?;
            let receiver = Receiver::new(settings.limits, sketcher);
            Ok(Self::Receiver(receiver))
        }
    }

    fn router(&self) -> axum::Router {
        match self {
            Self::Receiver(receiver) => receiver.clone().router(),
            Self::Session(session) => session.router(),
        }
    }

    async fn drain(&self) -> Result<()> {
        match self {
            Self::Receiver(receiver) => {
                receiver.close_and_drain().await;
                Ok(())
            }
            Self::Session(session) => session
                .shutdown()
                .await
                .map_err(|error| eyre!("session shutdown failed: {error}")),
        }
    }
}

#[cfg(unix)]
/// Shutdown signal listeners for the current platform.
pub struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    /// Install shutdown signal listeners.
    ///
    /// # Errors
    /// Returns an error if a listener cannot be installed.
    pub fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }
    /// Wait for a shutdown signal.
    ///
    /// # Errors
    /// Returns an error if signal handling fails.
    pub async fn wait(&mut self) -> Result<()> {
        tokio::select! {
            _ = self.interrupt.recv() => {
                Ok(())
            },
            _ = self.terminate.recv() => {
                Ok(())
            },
        }
    }
}

#[cfg(windows)]
/// Shutdown signal listener for the current platform.
pub struct Signals {
    interrupt: tokio::signal::windows::CtrlC,
}
#[cfg(windows)]
impl Signals {
    /// Install a shutdown signal listener.
    ///
    /// # Errors
    /// Returns an error if the listener cannot be installed.
    pub fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::windows::ctrl_c()?,
        })
    }
    /// Wait for a shutdown signal.
    ///
    /// # Errors
    /// Returns an error if signal handling fails.
    pub async fn wait(&mut self) -> Result<()> {
        let _ = self.interrupt.recv().await;
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
/// Placeholder for unsupported shutdown signal handling.
pub struct Signals;
#[cfg(not(any(unix, windows)))]
impl Signals {
    /// Attempt to install shutdown signal listeners.
    ///
    /// # Errors
    /// Always returns an error on unsupported platforms.
    pub fn new() -> Result<Self> {
        Err(eyre!("shutdown signals are unsupported on this platform"))
    }
    /// Wait for a shutdown signal.
    ///
    /// # Errors
    /// Always returns an error on unsupported platforms.
    pub async fn wait(&mut self) -> Result<()> {
        Err(eyre!("shutdown signals are unsupported on this platform"))
    }
}

/// Resolved settings for a server invocation.
pub struct Settings {
    /// Address on which to listen.
    pub(crate) listen: SocketAddr,
    /// Sample shared by uploads.
    pub(crate) sample: String,
    /// Parameters validated by the scientific sketching constructors.
    pub(crate) params: SketchParams,
    /// Ingestion batch limits.
    pub(crate) limits: Limits,
    /// Reference profiling, when enabled.
    pub(crate) profiling: Option<Profiling>,
    /// Loaded configuration file, if any.
    pub(crate) config_path: Option<PathBuf>,
}

/// Validated settings for reference profiling.
pub struct Profiling {
    reference: PathBuf,
    workers: NonZeroUsize,
    interval: Duration,
}

impl Profiling {
    /// Creates profiling settings with positive worker count and refresh interval.
    ///
    /// # Errors
    /// Returns an error if either the worker count or interval is zero.
    pub fn new(reference: PathBuf, workers: usize, interval: Duration) -> Result<Self> {
        if interval.is_zero() {
            return Err(eyre!(
                "profile workers and refresh interval must be positive"
            ));
        }
        let workers = NonZeroUsize::new(workers)
            .ok_or_else(|| eyre!("profile workers and refresh interval must be positive"))?;
        Ok(Self {
            reference,
            workers,
            interval,
        })
    }
}

/// Run the receiver until shutdown, then drain accepted work.
///
/// # Errors
/// Returns an error if initialization, serving, or shutdown fails.
pub async fn run(settings: Settings, display: &mut crate::display::Display) -> Result<()> {
    let mut signals = Signals::new().wrap_err("install shutdown signals")?;
    let owner = Owner::new(&settings).await?;
    let mut drivers = JoinSet::new();
    let mut warnings = Warnings::default();
    let result = serve(
        &settings,
        &owner,
        &mut signals,
        &mut drivers,
        display,
        &mut warnings,
    )
    .await;
    let receiver = match &owner {
        Owner::Receiver(receiver) => receiver.clone(),
        Owner::Session(session) => session.receiver(),
    };
    let mut observations = match &owner {
        Owner::Session(session) => Some(Box::pin(session.observe())),
        Owner::Receiver(_) => None,
    };
    let mut observation = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let drain = owner.drain();
    tokio::pin!(drain);
    let drain = loop {
        tokio::select! {
            result = &mut drain => {
                break result;
            },
            update = async {
                match &mut observations {
                    Some(stream) => stream.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(update) = update {
                    warnings.report(display, &receiver.ingestion_status(), Some(&update));
                    observation = Some(update);
                } else {
                    observations = None;
                }
            },
            _ = tick.tick() => {
                let status = receiver.ingestion_status();
                warnings.report(display, &status, observation.as_deref());
                if display.active() {
                    let lines = crate::display::server_lines(
                        status.processed.reads(),
                        status.processed.bases(),
                        observation.as_deref(),
                        settings.profiling.is_some(),
                        status.ingestion_error.is_some(),
                        false,
                    );
                    display.update(lines, false);
                }
            },
        }
    };
    let status = receiver.ingestion_status();
    let observation = match &owner {
        Owner::Session(session) => Box::pin(session.observe()).next().await,
        Owner::Receiver(_) => None,
    };
    warnings.report(display, &status, observation.as_deref());
    let lines = crate::display::server_lines(
        status.processed.reads(),
        status.processed.bases(),
        observation.as_deref(),
        settings.profiling.is_some(),
        status.ingestion_error.is_some(),
        false,
    );
    display.update(lines, true);
    drivers.abort_all();
    while drivers.join_next().await.is_some() {}
    match (result, drain) {
        (Err(error), Err(drain)) => Err(error.wrap_err(format!("drain also failed: {drain:#}"))),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), result) => result,
    }
}

async fn serve(
    settings: &Settings,
    owner: &Owner,
    signals: &mut Signals,
    drivers: &mut JoinSet<Result<(), String>>,
    display: &mut crate::display::Display,
    warnings: &mut Warnings,
) -> Result<()> {
    let listener = tokio::select! {
        biased;
        signal = signals.wait() => {
            signal?;
            return Ok(());
        },
        listener = TcpListener::bind(settings.listen) => {
            listener.wrap_err_with(|| format!("could not listen on {}", settings.listen))?
        },
    };
    tokio::select! {
        biased;
        signal = signals.wait() => {
            signal?;
            return Ok(());
        },
        () = std::future::ready(()) => {},
    };
    startup(settings, display, &listener)?;
    let router = owner.router();
    let receiver = match owner {
        Owner::Receiver(receiver) => receiver.clone(),
        Owner::Session(session) => session.receiver(),
    };
    let mut observations = match owner {
        Owner::Session(session) => Some(Box::pin(session.observe())),
        Owner::Receiver(_) => None,
    };
    let mut observation = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            biased;
            signal = signals.wait() => {
                break signal;
            },
            Some(result) = drivers.join_next(), if !drivers.is_empty() => {
                match result {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => display.message(&format!("connection error: {error}")),
                    Err(error) => display.message(&format!("connection task failed: {error}")),
                }
            },
            update = async {
                match &mut observations {
                    Some(stream) => stream.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(update) = update {
                    warnings.report(display, &receiver.ingestion_status(), Some(&update));
                    observation = Some(update);
                } else {
                    observations = None;
                }
            },
            _ = tick.tick() => {
                let status = receiver.ingestion_status();
                warnings.report(display, &status, observation.as_deref());
                if display.active() {
                    let lines = crate::display::server_lines(
                        status.processed.reads(),
                        status.processed.bases(),
                        observation.as_deref(),
                        settings.profiling.is_some(),
                        status.ingestion_error.is_some(),
                        true,
                    );
                    display.update(lines, false);
                }
            },
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let service = TowerToHyperService::new(router.clone());
                        drivers.spawn(async move {
                            let builder = Builder::new(TokioExecutor::new());
                            builder
                                .serve_connection(TokioIo::new(stream), service)
                                .await
                                .map_err(|error| error.to_string())
                        });
                    },
                    Err(error) => break Err(error).wrap_err("accept failed"),
                }
            },
        }
    }
}

fn startup(
    settings: &Settings,
    display: &crate::display::Display,
    listener: &TcpListener,
) -> Result<()> {
    #[cfg(feature = "hud")]
    if display.active() {
        let address = listener.local_addr()?.to_string();
        let reference = settings
            .profiling
            .as_ref()
            .map(|profiling| profiling.reference.display().to_string());
        let config = settings
            .config_path
            .as_ref()
            .map(|path| path.display().to_string());
        let sketch = format!(
            "c={}, k={}, dedup={}",
            settings.params.c,
            settings.params.k,
            matches!(
                settings.params.deduplication,
                sylph::read_sketch::Deduplication::Exact
            )
        );
        display.startup(&crate::display::StartupBanner {
            address: &address,
            sample: &settings.sample,
            reference: reference.as_deref(),
            sketch: &sketch,
            config: config.as_deref(),
            profiling: settings
                .profiling
                .as_ref()
                .map(|profiling| (profiling.workers.get(), profiling.interval.as_millis())),
        });
    } else {
        plain_startup(settings, display, listener)?;
    }
    #[cfg(not(feature = "hud"))]
    plain_startup(settings, display, listener)?;
    Ok(())
}

fn plain_startup(
    settings: &Settings,
    display: &crate::display::Display,
    listener: &TcpListener,
) -> Result<()> {
    let extra = settings.profiling.as_ref().map_or_else(
        || "no profiling or durable storage".to_owned(),
        |profiling| {
            format!(
                "reference={}, profile_workers={}, refresh_interval_ms={}; no durable storage",
                profiling.reference.display(),
                profiling.workers,
                profiling.interval.as_millis()
            )
        },
    );
    display.message(&format!(
        "Config     {}",
        settings.config_path.as_ref().map_or_else(
            || "defaults; no configuration file loaded".to_owned(),
            |path| path.display().to_string()
        )
    ));
    display.message(&format!(
        "Accumulating sample {:?} at http://{} (c={}, k={}, dedup={}); {}",
        settings.sample,
        listener.local_addr()?,
        settings.params.c,
        settings.params.k,
        matches!(
            settings.params.deduplication,
            sylph::read_sketch::Deduplication::Exact
        ),
        extra
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn profiling_requires_positive_workers_and_interval() {
        assert!(
            super::Profiling::new("reference".into(), 0, std::time::Duration::from_secs(1))
                .is_err()
        );
        assert!(super::Profiling::new("reference".into(), 1, std::time::Duration::ZERO).is_err());
        assert!(
            super::Profiling::new("reference".into(), 1, std::time::Duration::from_millis(1))
                .is_ok()
        );
    }
}
