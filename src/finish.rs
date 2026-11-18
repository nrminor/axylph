//! The bounded HTTP client for the final profile.

use std::{io::Write, process::ExitCode};

use color_eyre::eyre::{Context, Result, bail, ensure, eyre};
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode, Uri, body::Bytes, header};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpStream;

const MAX_BODY: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct Checkpoint {
    reads: String,
    bases: String,
}

impl Checkpoint {
    fn counts(&self) -> Result<(u64, u64)> {
        fn count(value: &str) -> Result<u64> {
            ensure!(
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
                "invalid checkpoint count"
            );
            Ok(value.parse()?)
        }
        let reads = count(&self.reads)?;
        let bases = count(&self.bases)?;
        Ok((reads, bases))
    }
}

#[derive(Deserialize)]
struct Failure {
    code: String,
    message: String,
}

#[derive(Deserialize)]
struct Finish {
    status: String,
    checkpoint: Checkpoint,
}

#[derive(Deserialize)]
struct Status {
    sample_name: String,
    state: String,
    processed: Checkpoint,
    profile_checkpoint: Checkpoint,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    ingestion_error: Option<Failure>,
    finish: Finish,
}

#[derive(Deserialize)]
struct Profile {
    sample_name: String,
    checkpoint: Checkpoint,
    rows: Vec<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct Snapshot {
    status: Status,
    profile: Profile,
}

fn validate(body: &[u8]) -> Result<Option<Failure>> {
    let snapshot: Snapshot =
        serde_json::from_slice(body).wrap_err("invalid finish response JSON")?;
    ensure!(
        snapshot.status.state == "stopped",
        "finish response is not stopped"
    );
    ensure!(
        snapshot.status.finish.status == "succeeded",
        "finish did not succeed"
    );
    ensure!(
        snapshot.status.sample_name == snapshot.profile.sample_name,
        "finish sample names differ"
    );
    let counts = snapshot.status.processed.counts()?;
    ensure!(
        snapshot.status.profile_checkpoint.counts()? == counts
            && snapshot.status.finish.checkpoint.counts()? == counts
            && snapshot.profile.checkpoint.counts()? == counts,
        "finish checkpoints differ"
    );
    let _ = snapshot.profile.rows;
    Ok(snapshot.status.ingestion_error)
}

fn output(body: &[u8], display: &crate::display::Display) -> Result<ExitCode> {
    let failure = validate(body)?;
    display.suspend_output(|| -> std::io::Result<()> {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        output.write_all(body)?;
        if !body.ends_with(b"\n") {
            output.write_all(b"\n")?;
        }
        output.flush()
    })?;
    if let Some(error) = failure {
        display.message(&format!(
            "warning: ingestion failed ({}): {}",
            error.code, error.message
        ));
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

fn port(authority: &hyper::http::uri::Authority) -> Result<u16> {
    let suffix = authority
        .as_str()
        .strip_prefix(authority.host())
        .ok_or_else(|| eyre!("invalid server authority"))?;
    if suffix.is_empty() {
        Ok(80)
    } else {
        let digits = suffix
            .strip_prefix(':')
            .ok_or_else(|| eyre!("invalid server port"))?;
        ensure!(
            !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()),
            "invalid server port"
        );
        digits.parse::<u16>().wrap_err("invalid server port")
    }
}

/// Request, validate, and print the server's bounded final-profile response.
pub async fn run(server: &str, display: &mut crate::display::Display) -> Result<ExitCode> {
    ensure!(
        !server.contains('#'),
        "server must be an HTTP origin without a fragment"
    );
    let uri: Uri = server.parse().wrap_err("invalid server URL")?;
    ensure!(uri.scheme_str() == Some("http"), "server must use http");
    let authority = uri
        .authority()
        .ok_or_else(|| eyre!("server must have a host"))?;
    ensure!(
        !authority.as_str().contains('@') && !authority.host().is_empty(),
        "server must not contain userinfo"
    );
    ensure!(
        uri.query().is_none() && (uri.path().is_empty() || uri.path() == "/"),
        "server must be an HTTP origin without a path or query"
    );
    let port = port(authority)?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let stream = TcpStream::connect((host, port))
        .await
        .wrap_err("could not connect to finish server")?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let request = Request::post("/finish")
        .header(header::HOST, authority.as_str())
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONNECTION, "close")
        .body(Full::new(Bytes::from_static(b"{}")))?;
    let operation = async {
        let response = sender.send_request(request).await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        let json = content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
        if let Some(length) = response.headers().get(header::CONTENT_LENGTH) {
            let length: u64 = length.to_str()?.parse()?;
            ensure!(
                length <= u64::try_from(MAX_BODY)?,
                "finish response exceeds 64 MiB"
            );
        }
        let mut incoming = response.into_body();
        let mut body = Vec::new();
        while let Some(frame) = incoming.frame().await {
            let frame = frame?;
            if let Ok(data) = frame.into_data() {
                ensure!(
                    data.len() <= MAX_BODY.saturating_sub(body.len()),
                    "finish response exceeds 64 MiB"
                );
                body.extend_from_slice(&data);
            }
        }
        if status != StatusCode::OK {
            let detail = serde_json::from_slice::<serde_json::Value>(&body).ok();
            let error = detail
                .as_ref()
                .and_then(|value| value.get("error"))
                .unwrap_or_else(|| detail.as_ref().unwrap_or(&serde_json::Value::Null));
            if let (Some(code), Some(message)) = (
                error.get("code").and_then(|v| v.as_str()),
                error.get("message").and_then(|v| v.as_str()),
            ) {
                bail!(
                    "finish server returned {status}: {}: {}",
                    code.chars().take(256).collect::<String>(),
                    message.chars().take(1024).collect::<String>()
                );
            }
            bail!("finish server returned {status}");
        }
        ensure!(json, "finish response is not application/json");
        let result = output(&body, display)?;
        display.update(vec!["Final profile received".to_owned()], true);
        Ok(result)
    };
    let mut connection = Box::pin(connection);
    tokio::pin!(operation);
    tokio::select! {
        result = &mut operation => result,
        result = &mut connection => {
            // Dropping the completed driver closes its channels, so an incomplete
            // response cannot leave the operation waiting for more data.
            drop(connection);
            match result {
                Ok(()) => operation.await,
                Err(error) => operation.await.wrap_err_with(|| format!("finish connection failed: {error}")),
            }
        }
    }
}
