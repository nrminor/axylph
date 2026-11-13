//! Executable-owned listeners and shutdown lifecycle.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use axylph::{Limits, Receiver, Session};
use color_eyre::eyre::{Result, WrapErr, eyre};
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

impl Owner {
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
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }
    async fn wait(&mut self) -> Result<()> {
        tokio::select! { _ = self.interrupt.recv() => Ok(()), _ = self.terminate.recv() => Ok(()) }
    }
}

#[cfg(windows)]
struct Signals {
    interrupt: tokio::signal::windows::CtrlC,
}
#[cfg(windows)]
impl Signals {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::windows::ctrl_c()?,
        })
    }
    async fn wait(&mut self) -> Result<()> {
        let _ = self.interrupt.recv().await;
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
struct Signals;
#[cfg(not(any(unix, windows)))]
impl Signals {
    fn new() -> Result<Self> {
        Err(eyre!("shutdown signals are unsupported on this platform"))
    }
    async fn wait(&mut self) -> Result<()> {
        Err(eyre!("shutdown signals are unsupported on this platform"))
    }
}

pub struct Settings {
    pub listen: SocketAddr,
    pub sample: String,
    pub params: SketchParams,
    pub limits: Limits,
    pub reference: Option<PathBuf>,
    pub workers: usize,
    pub interval_ms: u64,
}

pub async fn run(settings: Settings) -> Result<()> {
    let mut signals = Signals::new().wrap_err("install shutdown signals")?;
    let owner = if let Some(reference) = &settings.reference {
        let path = reference.clone();
        let workers = settings.workers;
        let profiler = tokio::task::spawn_blocking(move || {
            Profiler::open_syldb(&path, ProfileOptions::default(), workers)
        })
        .await
        .wrap_err("reference loader task failed")?
        .wrap_err_with(|| format!("could not load reference {}", reference.display()))?;
        Owner::Session(
            Session::builder(&settings.sample, profiler)
                .limits(settings.limits)
                .sketch_params(settings.params)
                .refresh_interval(Duration::from_millis(settings.interval_ms))
                .start()?,
        )
    } else {
        Owner::Receiver(Receiver::new(
            settings.limits,
            ReadSketcher::new(
                settings.params,
                "axylph".to_owned(),
                Some(settings.sample.clone()),
            )?,
        ))
    };
    let mut drivers = JoinSet::new();
    let result = serve(&settings, &owner, &mut signals, &mut drivers).await;
    let drain = owner.drain().await;
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
) -> Result<()> {
    let listener = tokio::select! {
        biased;
        signal = signals.wait() => { signal?; return Ok(()); },
        listener = TcpListener::bind(settings.listen) => listener.wrap_err_with(|| format!("could not listen on {}", settings.listen))?,
    };
    tokio::select! { biased; signal = signals.wait() => { signal?; return Ok(()); }, () = std::future::ready(()) => {} }
    let extra = settings.reference.as_ref().map_or_else(
        || "no profiling or durable storage".to_owned(),
        |path| {
            format!(
                "reference={}, profile_workers={}, refresh_interval_ms={}; no durable storage",
                path.display(),
                settings.workers,
                settings.interval_ms
            )
        },
    );
    eprintln!(
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
    );
    let router = owner.router();
    loop {
        tokio::select! {
            biased;
            signal = signals.wait() => break signal,
            Some(result) = drivers.join_next(), if !drivers.is_empty() => {
                match result { Ok(Ok(())) => {}, Ok(Err(error)) => eprintln!("connection error: {error}"), Err(error) => eprintln!("connection task failed: {error}") }
            },
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let service = TowerToHyperService::new(router.clone());
                    drivers.spawn(async move {
                        let builder = Builder::new(TokioExecutor::new());
                        builder.serve_connection(TokioIo::new(stream), service).await.map_err(|error| error.to_string())
                    });
                },
                Err(error) => break Err(error).wrap_err("accept failed"),
            }
        }
    }
}
