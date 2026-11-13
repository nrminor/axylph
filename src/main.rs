//! Command-line interface for read transfer and cumulative sketching.

mod config;

use std::fs::File;

use axylph::{Client, Receiver};
use color_eyre::eyre::WrapErr;
use config::Command;
use sylph::read_sketch::{Deduplication, ReadSketcher, SketchParams};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = config::parse()?;
    let limits = cli.limits;
    match cli.command {
        Command::Schema => config::print_schema()?,
        Command::Serve {
            listen,
            sample,
            c,
            k,
            no_dedup,
        } => {
            let sketcher = ReadSketcher::new(
                SketchParams {
                    c,
                    k,
                    deduplication: if no_dedup {
                        Deduplication::Disabled
                    } else {
                        Deduplication::Exact
                    },
                },
                "axylph".to_owned(),
                Some(sample.clone()),
            )?;
            let listener = TcpListener::bind(listen)
                .await
                .wrap_err_with(|| format!("could not listen on {listen}"))?;
            eprintln!(
                "Accumulating sample {sample:?} at http://{} (c={c}, k={k}, dedup={}); no profiling or durable storage",
                listener.local_addr()?,
                !no_dedup
            );
            axum::serve(listener, Receiver::new(limits, sketcher).router())
                .with_graceful_shutdown(async {
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        eprintln!("Cannot wait for shutdown signal: {error}");
                    }
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
    Ok(())
}
