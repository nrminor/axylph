//! Configuration precedence through the public binary and Flight upload seams.

use std::{
    env, fs,
    io::Cursor,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axylph::{Client, Limits, Progress};
use color_eyre::eyre::{Result, WrapErr, bail, ensure, eyre};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{ChildStderr, Command},
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(5);
static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
type ErrorCase<'a> = (&'a [&'a str], &'a [(&'a str, &'a str)], &'a str);

fn test_directory() -> Result<PathBuf> {
    let temporary = env::temp_dir();
    let approved = temporary.join("opencode");
    let base = if approved.is_dir() {
        approved
    } else {
        temporary
    };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let path = base.join(format!(
        "axylph-cli-{}-{}-{}",
        std::process::id(),
        timestamp.as_nanos(),
        DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&path).wrap_err_with(|| format!("create test directory {}", path.display()))?;
    Ok(path)
}

// Cleanup must run even after a failed assertion, without hiding the original error.
fn preserve_failure(outcome: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (outcome, cleanup) {
        (Err(original), Err(cleanup)) => {
            Err(original.wrap_err(format!("cleanup also failed: {cleanup:#}")))
        }
        (Err(original), Ok(())) => Err(original),
        (Ok(()), cleanup) => cleanup,
    }
}

fn binary(directory: &Path, args: &[&str], environment: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_axylph"));
    command
        .args(args)
        .current_dir(directory)
        .env_clear()
        .envs(environment.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn output(directory: &Path, args: &[&str], environment: &[(&str, &str)]) -> Result<Output> {
    let mut child = binary(directory, args, environment).spawn()?;
    let stdout = child.stdout.take().ok_or_else(|| eyre!("missing stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| eyre!("missing stderr"))?;
    let outcome = timeout(DEADLINE, async {
        let mut stdout = stdout.take(65_536);
        let mut stderr = stderr.take(65_536);
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let (stdout_result, stderr_result, status) = tokio::join!(
            stdout.read_to_end(&mut stdout_bytes),
            stderr.read_to_end(&mut stderr_bytes),
            child.wait(),
        );
        stdout_result?;
        stderr_result?;
        Ok(Output {
            status: status?,
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        })
    })
    .await;
    match outcome {
        Ok(result) => result,
        Err(error) => {
            timeout(DEADLINE, child.kill())
                .await
                .wrap_err("kill/reap timed-out CLI")??;
            Err(error).wrap_err_with(|| format!("CLI did not exit: {args:?}"))
        }
    }
}

async fn startup(
    directory: &Path,
    args: &[&str],
    environment: &[(&str, &str)],
    expected: &[&str],
) -> Result<()> {
    let mut child = binary(directory, args, environment).spawn()?;
    let outcome = timeout(DEADLINE, async {
        let stderr = child.stderr.take().ok_or_else(|| eyre!("missing stderr"))?;
        let mut lines = BufReader::new(stderr.take(16_384)).lines();
        let mut diagnostics = String::new();
        while let Some(line) = lines.next_line().await? {
            diagnostics.push_str(&line);
            diagnostics.push('\n');
            if line.starts_with("Accumulating sample ") {
                for fragment in expected {
                    ensure!(
                        line.contains(fragment),
                        "missing {fragment:?} in startup: {line}"
                    );
                }
                return Ok(());
            }
        }
        bail!("no startup announcement for {args:?}: {diagnostics}")
    })
    .await
    .wrap_err("CLI must start within five seconds")
    .and_then(|result| result);
    let cleanup = timeout(DEADLINE, child.kill())
        .await
        .wrap_err("timed out killing/reaping CLI")
        .and_then(|result| result.wrap_err("kill/reap CLI"));
    preserve_failure(outcome, cleanup)
}

async fn verify_startup_and_upload(stderr: ChildStderr) -> Result<()> {
    // Cap diagnostics even if a broken binary writes an unterminated line forever.
    let mut lines = BufReader::new(stderr.take(16_384)).lines();
    let mut diagnostics = String::new();
    let summary = loop {
        let Some(line) = lines
            .next_line()
            .await
            .wrap_err("read server startup stderr")?
        else {
            bail!("server closed stderr or exceeded 16 KiB before startup; stderr:\n{diagnostics}");
        };
        diagnostics.push_str(&line);
        diagnostics.push('\n');
        if line.starts_with("Accumulating sample ") {
            break line;
        }
    };
    ensure!(
        summary.contains("sample \"env-sample\" at "),
        "environment must override file sample: {summary}"
    );
    ensure!(
        summary.contains("c=7,"),
        "file sampling factor must reach startup: {summary}"
    );
    ensure!(
        summary.contains("k=31,"),
        "explicit k must override environment and invalid file k: {summary}"
    );
    let endpoint = summary
        .split_whitespace()
        .find(|word| word.starts_with("http://"))
        .ok_or_else(|| eyre!("startup omitted Flight endpoint: {summary}"))?;
    let address: SocketAddr = endpoint
        .trim_start_matches("http://")
        .parse()
        .wrap_err_with(|| format!("parse advertised Flight endpoint: {summary}"))?;
    ensure!(
        address.ip().is_loopback() && address.port() != 0,
        "expected allocated loopback endpoint: {address}"
    );
    let progress = Client::new(endpoint.to_owned(), Limits::default())
        .send(Cursor::new(b"@one\nACGT\n+\nIIII\n"), |_| {})
        .await
        .wrap_err("upload one read to configured binary's Flight service")?;
    ensure!(
        progress
            == Progress {
                reads: 1,
                bases: 4,
                complete: true
            },
        "unexpected Flight progress: {progress:?}"
    );
    Ok(())
}

async fn run_configured_binary(directory: &Path) -> Result<()> {
    let config = directory.join("axylph.toml");
    fs::write(
        &config,
        "[serve]\nsample = \"file-sample\"\nlisten = \"127.0.0.1:0\"\nc = 7\nk = 22\n",
    )
    .wrap_err("write configuration fixture")?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_axylph"))
        .arg("--config")
        .arg(&config)
        .args(["--k", "31"])
        .current_dir(directory)
        .env_clear()
        .env("AXYLPH_SAMPLE", "env-sample")
        .env("AXYLPH_K", "21")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .wrap_err("spawn bare axylph with configuration overrides")?;
    let outcome = async {
        let stderr = child.stderr.take().ok_or_else(|| eyre!("spawned binary has no piped stderr"))?;
        timeout(DEADLINE, verify_startup_and_upload(stderr)).await
            .wrap_err("configured binary must announce startup and accept Flight upload within five seconds")?
    }.await;
    // kill() also waits/reaps; kill_on_drop is a fallback if cleanup itself times out.
    let cleanup = timeout(DEADLINE, child.kill())
        .await
        .wrap_err("timed out killing/reaping configured binary")
        .and_then(|result| result.wrap_err("kill/reap configured binary"));
    preserve_failure(outcome, cleanup)
}

#[tokio::test]
async fn bare_command_merges_file_environment_and_explicit_flags_before_starting_flight()
-> Result<()> {
    let directory = test_directory()?;
    let outcome = run_configured_binary(&directory).await;
    let cleanup = fs::remove_dir_all(&directory)
        .wrap_err_with(|| format!("remove test directory {}", directory.display()));
    preserve_failure(outcome, cleanup)
}

#[tokio::test]
async fn config_selection_uses_one_file_and_explicit_serve_flags_override_boolean_sources()
-> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        fs::write(
            directory.join("axylph.toml"),
            "[serve]\nsample = 'cwd'\nc = 7\ndedup = false\n",
        )?;
        fs::write(
            directory.join("env.toml"),
            "[serve]\nsample = 'env-file'\ndedup = false\n",
        )?;
        fs::write(
            directory.join("arg.toml"),
            "[serve]\nsample = 'arg-file'\ndedup = true\n",
        )?;
        // The absent c in the selected file must use the default, not another file's c.
        startup(
            &directory,
            &[
                "serve",
                "--listen",
                "127.0.0.1:0",
                "--config",
                "arg.toml",
                "--no-dedup",
            ],
            &[("AXYLPH_CONFIG", "env.toml"), ("AXYLPH_DEDUP", "true")],
            &["sample \"arg-file\"", "c=200,", "k=31,", "dedup=false"],
        )
        .await?;
        startup(
            &directory,
            &["serve", "--listen", "127.0.0.1:0", "--dedup"],
            &[("AXYLPH_CONFIG", "env.toml"), ("AXYLPH_DEDUP", "false")],
            &["sample \"env-file\"", "c=200,", "dedup=true"],
        )
        .await?;
        startup(
            &directory,
            &["serve", "--listen", "127.0.0.1:0"],
            &[],
            &["sample \"cwd\"", "c=7,", "dedup=false"],
        )
        .await?;
        startup(
            &directory,
            &["serve", "--listen", "127.0.0.1:0"],
            &[("AXYLPH_DEDUP", "true")],
            &["sample \"cwd\"", "dedup=true"],
        )
        .await?;
        fs::remove_file(directory.join("axylph.toml"))?;
        startup(
            &directory,
            &["serve", "--listen", "127.0.0.1:0", "--sample", "args-only"],
            &[],
            &["sample \"args-only\"", "c=200,", "k=31,", "dedup=true"],
        )
        .await
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(&directory).wrap_err("remove CLI fixtures"),
    )
}

#[tokio::test]
async fn invalid_configuration_and_serve_flags_on_send_fail_with_relevant_diagnostics() -> Result<()>
{
    let directory = test_directory()?;
    let outcome = async {
        fs::write(directory.join("unknown.toml"), "batch_readz = 1\n")?;
        fs::write(directory.join("nested.toml"), "[serve]\nunknown = true\n")?;
        fs::write(directory.join("malformed.toml"), "[serve\n")?;
        fs::write(
            directory.join("invalid.toml"),
            "[serve]\nsample = 'test'\nk = 22\n",
        )?;
        let cases: &[ErrorCase<'_>] = &[
            (&["--config", "missing.toml"], &[], "missing.toml"),
            (
                &[],
                &[("AXYLPH_CONFIG", "missing-env.toml")],
                "missing-env.toml",
            ),
            (&["--config", "unknown.toml"], &[], "batch_readz"),
            (&["--config", "nested.toml"], &[], "unknown"),
            (&["--config", "malformed.toml"], &[], "malformed.toml"),
            (&["--config", "invalid.toml"], &[], "21"),
            (&["--sample", "test", "--k", "22"], &[], "21"),
            (&["--sample", "test", "--batch-reads", "0"], &[], "batch"),
        ];
        for (args, environment, diagnostic) in cases {
            assert_failure(&directory, args, environment, diagnostic).await?;
        }
        for flags in [
            vec!["--sample", "test"],
            vec!["--listen", "127.0.0.1:0"],
            vec!["--c", "7"],
            vec!["--k", "31"],
            vec!["--dedup"],
            vec!["--no-dedup"],
        ] {
            let mut args = vec!["send", "missing.fastq"];
            args.extend(flags);
            assert_failure(&directory, &args, &[], "serve").await?;
        }
        Ok(())
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(&directory).wrap_err("remove CLI fixtures"),
    )
}

async fn assert_failure(
    directory: &Path,
    args: &[&str],
    environment: &[(&str, &str)],
    diagnostic: &str,
) -> Result<()> {
    let result = output(directory, args, environment).await?;
    let stderr = String::from_utf8(result.stderr)?;
    ensure!(!result.status.success(), "unexpected CLI success: {args:?}");
    ensure!(
        stderr.contains(diagnostic),
        "expected {diagnostic:?} for {args:?}, stderr: {stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn send_requires_no_sample_and_does_not_validate_inactive_serve_values() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        fs::write(
            directory.join("axylph.toml"),
            "[serve]\nk = 22\nc = 0\nlisten = 'invalid'\n",
        )?;
        // A missing input is intentional: reaching File::open proves configuration resolved
        // without opening a network connection or accidentally validating serve's values.
        assert_failure(
            &directory,
            &["send", "missing.fastq"],
            &[("AXYLPH_K", "22"), ("AXYLPH_LISTEN", "invalid")],
            "could not open missing.fastq",
        )
        .await
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(&directory).wrap_err("remove CLI fixtures"),
    )
}

#[tokio::test]
async fn schema_prints_draft7_partial_config_json_without_loading_configuration() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        fs::write(directory.join("axylph.toml"), "not valid TOML [")?;
        let result = output(
            &directory,
            &["--config", "missing.toml", "schema"],
            &[
                ("AXYLPH_CONFIG", "also-missing.toml"),
                ("AXYLPH_BATCH_READS", "invalid"),
            ],
        )
        .await?;
        ensure!(
            result.status.success(),
            "schema failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let schema: serde_json::Value = serde_json::from_slice(&result.stdout)?;
        ensure!(
            schema.get("$schema").and_then(serde_json::Value::as_str)
                == Some("http://json-schema.org/draft-07/schema#"),
            "schema must declare Draft 7: {schema}"
        );
        assert_partial_object(&schema)?;
        let definitions = schema
            .get("definitions")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| eyre!("Draft 7 schema has no definitions"))?;
        ensure!(
            !definitions.is_empty(),
            "schema must describe nested configuration"
        );
        for definition in definitions.values() {
            if definition.get("type").and_then(serde_json::Value::as_str) == Some("object") {
                assert_partial_object(definition)?;
            }
        }
        Ok(())
    }
    .await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(&directory).wrap_err("remove CLI fixtures"),
    )
}

async fn generated_sample_and_upload(stderr: ChildStderr) -> Result<String> {
    let mut lines = BufReader::new(stderr.take(16_384)).lines();
    let mut diagnostics = String::new();
    let summary = loop {
        let Some(line) = lines.next_line().await? else {
            bail!("no generated-sample startup announcement; stderr:\n{diagnostics}");
        };
        diagnostics.push_str(&line);
        diagnostics.push('\n');
        if line.starts_with("Accumulating sample ") {
            break line;
        }
    };
    let (sample, endpoint) = summary
        .strip_prefix("Accumulating sample \"")
        .and_then(|rest| rest.split_once("\" at "))
        .ok_or_else(|| eyre!("malformed startup announcement: {summary}"))?;
    ensure!(
        sample
            .strip_prefix("sample-")
            .is_some_and(|suffix| !suffix.is_empty()),
        "expected a nonempty generated sample name: {summary}"
    );
    for default in ["c=200,", "k=31,", "dedup=true"] {
        ensure!(
            summary.contains(default),
            "missing default {default:?}: {summary}"
        );
    }
    let endpoint = endpoint
        .split_whitespace()
        .next()
        .ok_or_else(|| eyre!("missing Flight endpoint: {summary}"))?;
    let address: SocketAddr = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| eyre!("invalid Flight endpoint: {summary}"))?
        .parse()?;
    ensure!(
        address.ip().is_loopback() && address.port() != 0,
        "expected allocated loopback endpoint: {address}"
    );
    let progress = Client::new(endpoint.to_owned(), Limits::default())
        .send(Cursor::new(b"@one\nACGT\n+\nIIII\n"), |_| {})
        .await
        .wrap_err("upload one read to generated-sample receiver")?;
    ensure!(
        progress
            == Progress {
                reads: 1,
                bases: 4,
                complete: true
            },
        "unexpected generated-sample Flight progress: {progress:?}"
    );
    Ok(sample.to_owned())
}

#[tokio::test]
async fn missing_sample_generates_different_names_and_accepts_flight_uploads() -> Result<()> {
    let directory = test_directory()?;
    let outcome = async {
        let mut names = Vec::new();
        for args in [&[][..], &["serve"][..]] {
            let mut child = binary(&directory, args, &[("AXYLPH_LISTEN", "127.0.0.1:0")])
                .spawn()?;
            let outcome = async {
                let stderr = child.stderr.take().ok_or_else(|| eyre!("missing stderr"))?;
                let name = timeout(DEADLINE, generated_sample_and_upload(stderr))
                    .await
                    .wrap_err("generated-sample receiver must start and accept upload within five seconds")??;
                names.push(name);
                Ok(())
            }.await;
            let cleanup = timeout(DEADLINE, child.kill())
                .await
                .wrap_err("timed out killing/reaping generated-sample receiver")
                .and_then(|result| result.wrap_err("kill/reap generated-sample receiver"));
            preserve_failure(outcome, cleanup)?;
        }
        ensure!(names[0] != names[1], "separate runs reused generated sample name: {names:?}");
        Ok(())
    }.await;
    preserve_failure(
        outcome,
        fs::remove_dir_all(&directory).wrap_err("remove CLI fixtures"),
    )
}

fn assert_partial_object(schema: &serde_json::Value) -> Result<()> {
    ensure!(
        schema.get("additionalProperties") == Some(&serde_json::Value::Bool(false)),
        "configuration object must deny unknown fields: {schema}"
    );
    ensure!(
        schema
            .get("required")
            .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty)),
        "partial configuration must not require fields: {schema}"
    );
    ensure!(
        schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|properties| !properties.is_empty()),
        "configuration object must describe its fields: {schema}"
    );
    Ok(())
}
