//! Command-line interface for read transfer and validation.

use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;

use axylph::{Client, Error, Limits, Receiver};
use clap::{Parser, Subcommand};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(
    version,
    about = "Transfer sequencing reads. This prototype does not run sylph."
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
    /// Receive and validate reads on a trusted network. No authentication or TLS.
    Serve {
        /// Address for the Flight service.
        #[arg(long, default_value = "127.0.0.1:50051")]
        listen: SocketAddr,
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
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let limits = Limits::new(cli.batch_bytes, cli.batch_reads)?;
    match cli.command {
        Command::Serve { listen } => {
            let listener = TcpListener::bind(listen).await?;
            eprintln!(
                "Receiving reads at http://{}; no profiling or storage",
                listener.local_addr()?
            );
            axum::serve(listener, Receiver::new(limits).router())
                .with_graceful_shutdown(async {
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        eprintln!("Cannot wait for shutdown signal: {error}");
                    }
                })
                .await?;
        }
        Command::Send { path, server } => {
            Client::new(server, limits)
                .send(File::open(path)?, |progress| {
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
                .await?;
        }
    }
    Ok(())
}
