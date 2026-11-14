//! Public finish-command regressions against bounded loopback HTTP peers.

use std::{fs, path::Path, process::Output, time::Duration};

use color_eyre::eyre::{Result, WrapErr, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use super::{DEADLINE, output, preserve_failure, test_directory};

// Literal wire bytes, not JSON reserialized through a floating-point representation.
const SNAPSHOT: &str = concat!(
    "{\"status\":{\"sample_name\":\"synthetic-sample\",\"state\":\"stopped\",",
    "\"processed\":{\"reads\":\"9007199254740993\",\"bases\":\"18446744073709551615\"},",
    "\"profile_checkpoint\":{\"reads\":\"9007199254740993\",\"bases\":\"18446744073709551615\"},",
    "\"ingestion_error\":null,\"refresh_error\":null,\"finish\":{\"status\":\"succeeded\",",
    "\"checkpoint\":{\"reads\":\"9007199254740993\",\"bases\":\"18446744073709551615\"}}},",
    "\"profile\":{\"sample_name\":\"synthetic-sample\",",
    "\"checkpoint\":{\"reads\":\"9007199254740993\",\"bases\":\"18446744073709551615\"},",
    "\"rows\":[{\"adjusted_ani_fraction\":0.123456789012345678901234567890}]}}"
);

async fn read_finish_request(stream: &mut TcpStream) -> Result<()> {
    let mut reader = BufReader::new(stream.take(8_192));
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    ensure!(
        line == "POST /finish HTTP/1.1\r\n",
        "request line: {line:?}"
    );
    let mut json = false;
    let mut length = false;
    for _ in 0..32 {
        line.clear();
        ensure!(reader.read_line(&mut line).await? != 0, "request truncated");
        if line == "\r\n" {
            ensure!(
                json && length,
                "request must declare JSON and a two-byte body"
            );
            let mut body = [0_u8; 2];
            reader.read_exact(&mut body).await?;
            ensure!(body == *b"{}", "finish request body: {body:?}");
            return Ok(());
        }
        let header = line.to_ascii_lowercase();
        json |= header.trim_end() == "content-type: application/json";
        length |= header.trim_end() == "content-length: 2";
    }
    color_eyre::eyre::bail!("finish request exceeded header budget")
}

fn response(status: &str, content_type: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

async fn run_reply(
    directory: &Path,
    listener: TcpListener,
    args: &[&str],
    environment: &[(&str, &str)],
    wire: &[u8],
    keep_open: bool,
) -> Result<Output> {
    let peer = timeout(DEADLINE, async {
        let (mut stream, _) = listener.accept().await?;
        read_finish_request(&mut stream).await?;
        stream.write_all(wire).await?;
        // The join retains this stream until the CLI exits. A client that waits for
        // EOF instead of respecting HTTP framing will hit output's bounded timeout.
        Ok::<_, color_eyre::Report>(keep_open.then_some(stream))
    });
    let (result, peer) = tokio::join!(output(directory, args, environment), peer);
    let result = result?;
    peer.wrap_err_with(|| {
        format!(
            "loopback peer timed out; CLI status: {}, stderr: {}",
            result.status,
            String::from_utf8_lossy(&result.stderr)
        )
    })??;
    Ok(result)
}

async fn finish_reply(directory: &Path, wire: &[u8], keep_open: bool) -> Result<Output> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    run_reply(
        directory,
        listener,
        &["finish", "--server", &endpoint],
        &[],
        wire,
        keep_open,
    )
    .await
}

#[tokio::test]
async fn finish_preserves_scientific_digits_and_large_counters_without_waiting_for_eof()
-> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        let result = finish_reply(
            &directory,
            &response("200 OK", "application/json; charset=utf-8", SNAPSHOT),
            true,
        )
        .await?;
        ensure!(
            result.status.code() == Some(0),
            "finish failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        ensure!(
            result.stdout == format!("{SNAPSHOT}\n").as_bytes(),
            "stdout must preserve every response byte and append one newline"
        );
        ensure!(
            result.stderr.is_empty(),
            "successful finish must not warn: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        Ok(())
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove finish wire fixtures"),
    )
}

#[tokio::test]
async fn finish_with_ingestion_error_prints_original_json_warns_and_exits_nonzero() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        let body = SNAPSHOT.replace(
            "\"ingestion_error\":null",
            "\"ingestion_error\":{\"code\":\"invalid_fastq\",\"message\":\"quality length differs\"}",
        );
        let result = finish_reply(&directory, &response("200 OK", "application/json", &body), true).await?;
        ensure!(!result.status.success(), "ingestion error must fail the CLI");
        ensure!(result.stdout == format!("{body}\n").as_bytes(), "ingestion failure must retain original JSON");
        let stderr = String::from_utf8(result.stderr)?;
        ensure!(stderr.to_ascii_lowercase().contains("warning") && stderr.contains("invalid_fastq") && stderr.contains("quality length differs"), "missing ingestion warning: {stderr}");
        Ok(())
    }.await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove ingestion error fixtures"),
    )
}

#[tokio::test]
async fn finish_rejects_error_responses_and_invalid_terminal_snapshots_without_stdout() -> Result<()>
{
    let directory = test_directory()?;
    let outcome = async {
        let cases = [
            ("structured error", response("422 Unprocessable Entity", "application/json", r#"{"error":{"code":"refresh_failed","message":"independent refresh failure"}}"#), true),
            ("false gRPC success", response("200 OK", "application/grpc", SNAPSHOT), true),
            ("malformed JSON", response("200 OK", "application/json", "{"), true),
            ("truncated body", b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{}".to_vec(), false),
        ];
        for (name, wire, keep_open) in cases {
            let result = finish_reply(&directory, &wire, keep_open).await.wrap_err(name)?;
            ensure!(!result.status.success() && result.stdout.is_empty(), "{name}: rejection must not emit stdout");
            let stderr = String::from_utf8(result.stderr)?;
            ensure!(!stderr.is_empty(), "{name}: rejection needs diagnostics");
            if name == "structured error" {
                ensure!(stderr.contains("refresh_failed") && stderr.contains("independent refresh failure"), "structured error details lost: {stderr}");
            }
        }
        for (name, from, to) in [
            ("processed checkpoint", "\"processed\":{\"reads\":\"9007199254740993\"", "\"processed\":{\"reads\":\"9007199254740992\""),
            ("profile status checkpoint", "\"profile_checkpoint\":{\"reads\":\"9007199254740993\"", "\"profile_checkpoint\":{\"reads\":\"9007199254740992\""),
            ("finish checkpoint", "\"finish\":{\"status\":\"succeeded\",\"checkpoint\":{\"reads\":\"9007199254740993\"", "\"finish\":{\"status\":\"succeeded\",\"checkpoint\":{\"reads\":\"9007199254740992\""),
            ("profile checkpoint", "\"profile\":{\"sample_name\":\"synthetic-sample\",\"checkpoint\":{\"reads\":\"9007199254740993\"", "\"profile\":{\"sample_name\":\"synthetic-sample\",\"checkpoint\":{\"reads\":\"9007199254740992\""),
            ("nondecimal counter", "9007199254740993", "9e15"),
            ("overflowing counter", "18446744073709551615", "18446744073709551616"),
            ("profile sample", "\"profile\":{\"sample_name\":\"synthetic-sample\"", "\"profile\":{\"sample_name\":\"other\""),
            ("open status", "\"state\":\"stopped\"", "\"state\":\"open\""),
            ("unfinished status", "\"status\":\"succeeded\"", "\"status\":\"pending\""),
            ("missing profile", "\"profile\":{", "\"not_profile\":{"),
            ("rows must be array", "\"rows\":[{\"adjusted_ani_fraction\":0.123456789012345678901234567890}]", "\"rows\":{}"),
            ("invalid ingestion error", "\"ingestion_error\":null", "\"ingestion_error\":{\"code\":17,\"message\":false}"),
        ] {
            ensure!(SNAPSHOT.contains(from), "{name}: invalid test substitution");
            let body = SNAPSHOT.replace(from, to);
            let result = finish_reply(&directory, &response("200 OK", "application/json", &body), true).await.wrap_err(name)?;
            ensure!(!result.status.success() && result.stdout.is_empty(), "{name}: invalid snapshot must fail without stdout");
        }
        Ok(())
    }.await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove rejected response fixtures"),
    )
}

#[tokio::test]
async fn finish_server_precedence_ignores_inactive_semantic_configuration() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        for source in ["flag", "environment", "file"] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}/", listener.local_addr()?);
            let file_server = if source == "file" { &endpoint } else { "https://inactive.invalid" };
            fs::write(directory.join("axylph.toml"), format!(
                "batch_reads = 0\nbatch_bytes = 0\n[serve]\nk = 22\nc = 0\nlisten = 'invalid'\nprofile_workers = 0\nrefresh_interval_ms = 0\n[send]\nserver = 'https://wrong-send.invalid'\n[finish]\nserver = '{file_server}'\n"
            ))?;
            let mut environment = vec![("AXYLPH_K", "invalid"), ("AXYLPH_LISTEN", "invalid"), ("AXYLPH_BATCH_READS", "invalid"), ("AXYLPH_BATCH_BYTES", "invalid")];
            let mut args = vec!["finish"];
            if source == "flag" {
                args.extend(["--server", &endpoint]);
                environment.push(("AXYLPH_SERVER", "https://inactive.invalid"));
            } else if source == "environment" {
                environment.push(("AXYLPH_SERVER", &endpoint));
            }
            let result = run_reply(&directory, listener, &args, &environment, &response("200 OK", "application/json", SNAPSHOT), true).await.wrap_err(source)?;
            ensure!(result.status.code() == Some(0) && result.stdout == format!("{SNAPSHOT}\n").as_bytes(), "{source}: finish must use its active server only; stderr: {}", String::from_utf8_lossy(&result.stderr));
        }
        Ok(())
    }.await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove finish precedence fixtures"),
    )
}

#[tokio::test]
async fn finish_rejects_explicit_inactive_flags_and_nonorigin_urls_before_connecting() -> Result<()>
{
    let directory = test_directory()?;
    let outcome = async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}");
        for flags in [
            vec!["--batch-reads", "1"],
            vec!["--batch-bytes", "1024"],
            vec!["--sample", "wrong"],
            vec!["--listen", "127.0.0.1:0"],
            vec!["--reference", "missing.syldb"],
            vec!["--profile-workers", "1"],
            vec!["--refresh-interval-ms", "1"],
            vec!["--c", "20"],
            vec!["--k", "31"],
            vec!["--dedup"],
            vec!["--no-dedup"],
        ] {
            let mut args = vec!["finish", "--server", &endpoint];
            args.extend(flags);
            let result = output(&directory, &args, &[]).await?;
            ensure!(
                !result.status.success() && result.stdout.is_empty(),
                "inactive flags must fail without stdout: {args:?}"
            );
            ensure!(
                String::from_utf8_lossy(&result.stderr).contains("finish"),
                "inactive flag error must identify finish"
            );
        }
        for endpoint in [
            format!("https://{address}"),
            format!("http://user:secret@{address}"),
            format!("http://{address}/?query=1"),
            format!("http://{address}/#fragment"),
            format!("http://{address}/nested"),
            format!("http://{}:+80", address.ip()),
            format!("http://{}:+{}", address.ip(), address.port()),
            format!("http://{}:", address.ip()),
        ] {
            let result = output(&directory, &["finish", "--server", &endpoint], &[]).await?;
            ensure!(
                !result.status.success() && result.stdout.is_empty(),
                "nonorigin URL must fail without stdout: {endpoint}"
            );
            ensure!(
                !result.stderr.is_empty(),
                "invalid endpoint needs diagnostics"
            );
            if endpoint.contains(":+") || endpoint.ends_with(':') {
                ensure!(
                    String::from_utf8_lossy(&result.stderr).contains("invalid server port"),
                    "invalid port must fail validation, not merely fail to connect: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
            }
        }
        ensure!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err(),
            "invalid flags and origins must be rejected before connecting"
        );
        Ok(())
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove finish argument fixtures"),
    )
}

#[tokio::test]
async fn finish_caps_chunked_response_incrementally_without_stdout() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let peer = timeout(DEADLINE, async {
            let (mut stream, _) = listener.accept().await?;
            read_finish_request(&mut stream).await?;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n").await?;
            let chunk = [b' '; 8_192];
            for index in 0..8_193 {
                // Oversize rejection is allowed to close the socket mid-write.
                for bytes in [b"2000\r\n".as_slice(), &chunk, b"\r\n"] {
                    if let Err(error) = stream.write_all(bytes).await {
                        ensure!(matches!(error.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset), "unexpected peer write error: {error}");
                        ensure!(index >= 8_192, "client closed before the response reached the 64 MiB budget");
                        return Ok(stream);
                    }
                }
            }
            // Deliberately omit the terminating chunk and retain the socket in
            // join's result slot. Collect-to-completion clients must time out.
            Ok::<_, color_eyre::Report>(stream)
        });
        let args = ["finish", "--server", &endpoint];
        let (result, peer) = tokio::join!(output(&directory, &args, &[]), peer);
        peer.wrap_err("oversize chunked peer timed out")??;
        let result = result?;
        ensure!(!result.status.success() && result.stdout.is_empty(), "oversized body must fail without stdout");
        let stderr = String::from_utf8(result.stderr)?;
        ensure!(stderr.contains("67108864") || stderr.contains("64 MiB"), "oversize error must identify the response budget: {stderr}");
        Ok(())
    }.await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(directory).wrap_err("remove oversized response fixtures"),
    )
}
