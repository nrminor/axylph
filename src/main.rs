//! Command-line interface for read transfer and cumulative sketching.

use std::{fs::File, net::SocketAddr, path::PathBuf};

use axylph::{Client, Limits, Receiver};
use clap::{Parser, Subcommand};
use color_eyre::eyre::WrapErr;
use sylph::read_sketch::{Deduplication, ReadSketcher, SketchParams};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(
    version,
    about = "Accumulate sequencing read sketches. This prototype does not produce profiles."
)]
struct Cli {
    /// Maximum identifier, sequence, and quality bytes per decoded batch.
    #[arg(long, default_value_t = 1_048_576, global = true)]
    batch_bytes: usize,
    /// Maximum reads per batch.
    #[arg(long, default_value_t = 1024, global = true)]
    batch_reads: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Accumulate one sample on a trusted network. No authentication or TLS.
    Serve {
        /// Address for the Flight service.
        #[arg(long, default_value = "127.0.0.1:50051")]
        listen: SocketAddr,
        /// Sample shared by every upload to this receiver.
        #[arg(long)]
        sample: String,
        /// Sylph sampling factor (positive).
        #[arg(long, default_value_t = 200)]
        c: usize,
        /// K-mer length (21 or 31).
        #[arg(long, default_value_t = 31)]
        k: usize,
        /// Count every sampled occurrence instead of exact single-end deduplication.
        #[arg(long)]
        no_dedup: bool,
    },
    /// Send a plain or gzip FASTQ file and report validation progress.
    Send {
        /// Path to a four-line-per-read FASTQ file, optionally gzip compressed.
        path: PathBuf,
        /// HTTP address of the Flight service.
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let limits = Limits::new(cli.batch_bytes, cli.batch_reads)?;
    match cli.command {
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
