//! Command-line interface for read transfer and cumulative sketching.

mod config;
mod finish;
mod server;

use std::{fs::File, process::ExitCode};

use axylph::Client;
use color_eyre::eyre::WrapErr;
use config::Command;
use sylph::read_sketch::{Deduplication, SketchParams};

fn main() -> color_eyre::Result<ExitCode> {
    color_eyre::install()?;
    let cli = config::parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config::worker_count())
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(cli));
    drop(runtime);
    result
}

async fn run(cli: config::Resolved) -> color_eyre::Result<ExitCode> {
    let limits = cli.limits;
    match cli.command {
        Command::Schema => config::print_schema()?,
        Command::Finish { server } => return finish::run(&server).await,
        Command::Serve {
            listen,
            sample,
            c,
            k,
            no_dedup,
            reference,
            profile_workers,
            refresh_interval_ms,
        } => {
            server::run(server::Settings {
                listen,
                sample,
                limits,
                reference,
                workers: profile_workers,
                interval_ms: refresh_interval_ms,
                params: SketchParams {
                    c,
                    k,
                    deduplication: if no_dedup {
                        Deduplication::Disabled
                    } else {
                        Deduplication::Exact
                    },
                },
            })
            .await?;
        }
        Command::Send { path, server } => {
            let input =
                File::open(&path).wrap_err_with(|| format!("could not open {}", path.display()))?;
            Client::new(server, limits)
                .send(input, |progress| {
                    eprintln!(
                        "validated: {} reads, {} bases{}",
                        progress.reads,
                        progress.bases,
                        if progress.complete {
                            " (upload complete)"
                        } else {
                            ""
                        }
                    );
                })
                .await
                .wrap_err_with(|| format!("could not transfer {}", path.display()))?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
