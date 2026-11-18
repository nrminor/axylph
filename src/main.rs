//! Command-line interface for read transfer and cumulative sketching.

mod config;
mod display;
mod finish;
mod server;

use std::{fs::File, process::ExitCode};

use axylph::Client;
use color_eyre::eyre::WrapErr;
use config::Command;

fn main() -> color_eyre::Result<ExitCode> {
    color_eyre::config::HookBuilder::new()
        .theme(color_eyre::config::Theme::new())
        .install()?;
    let cli = config::parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config::worker_count())
        .enable_all()
        .build()?;
    runtime.block_on(run(cli))
}

async fn run(cli: config::Resolved) -> color_eyre::Result<ExitCode> {
    let limits = cli.limits;
    if matches!(cli.command, Command::Schema) {
        config::print_schema()?;
        return Ok(ExitCode::SUCCESS);
    }
    let mut display = display::Display::new(cli.display);
    match cli.command {
        Command::Schema => {}
        Command::Finish { server } => return finish_command(&server, &mut display).await,
        Command::Serve(settings) => {
            server::run(settings, &mut display).await?;
        }
        Command::Send { path, server } => {
            let input =
                File::open(&path).wrap_err_with(|| format!("could not open {}", path.display()))?;
            let client = Client::new(server, limits);
            if display.active() {
                let mut signals = server::Signals::new().wrap_err("install shutdown signals")?;
                display.update(
                    vec![
                        "Upload in progress".to_owned(),
                        "Sample not finalized".to_owned(),
                    ],
                    true,
                );
                let result = {
                    let send = client.send(input, |progress| display.progress(progress));
                    tokio::select! {
                        result = send => Some(result.wrap_err_with(|| format!("could not transfer {}", path.display()))),
                        result = signals.wait() => { result?; None }
                    }
                };
                match result {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        display.update(
                            vec![
                                "Upload failed".to_owned(),
                                "Accepted reads are not rolled back".to_owned(),
                            ],
                            true,
                        );
                        return Err(error);
                    }
                    None => {
                        display.update(
                            vec![
                                "Upload interrupted".to_owned(),
                                "Accepted reads are not rolled back".to_owned(),
                            ],
                            true,
                        );
                        return Ok(ExitCode::FAILURE);
                    }
                }
            } else {
                client
                    .send(input, |progress| display.progress(progress))
                    .await
                    .wrap_err_with(|| format!("could not transfer {}", path.display()))?;
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn finish_command(
    server: &str,
    display: &mut display::Display,
) -> color_eyre::Result<ExitCode> {
    if !display.active() {
        return finish::run(server, display).await;
    }
    let mut signals = server::Signals::new().wrap_err("install shutdown signals")?;
    display.update(vec!["Waiting for final profile".to_owned()], true);
    let result = tokio::select! {
        result = finish::run(server, display) => Some(result),
        result = signals.wait() => { result?; None }
    };
    match result {
        Some(Ok(code)) => Ok(code),
        Some(Err(error)) => {
            display.update(vec!["Finish failed".to_owned()], true);
            Err(error)
        }
        None => {
            display.update(
                vec![
                    "Finish interrupted".to_owned(),
                    "Server may continue finalizing".to_owned(),
                ],
                true,
            );
            Ok(ExitCode::FAILURE)
        }
    }
}
