//! Real Flight input and periodic, terminal, and SSE profiles against an independent oracle.
//!
//! `default.json` is a lossless YAML-to-JSON conversion of the sylph fork's pinned
//! `default.yaml` (SHA-256 8af09a9fbabe124676d25ae9d00a24e0e59bde756b28d16ab506b37617fef966).
//! The reference database is copied byte-for-byte. Neither oracle was regenerated.

#![cfg(test)]

use std::{
    fmt::Write, io::Cursor, net::SocketAddr, panic::AssertUnwindSafe, path::Path, time::Duration,
};

use axylph::{Client, Limits, Session};
use futures_util::{FutureExt, StreamExt};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{
    Request, StatusCode,
    body::{Bytes, Incoming},
    client::conn::http1,
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sylph::{
    profile::{Profile, ProfileOptions, Profiler, SequenceEstimate},
    read_sketch::Deduplication,
    types::AdjustStatus,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

#[path = "support/profile_oracle.rs"]
mod profile_oracle;

use profile_oracle::{Fixture, Prefix, assert_wire_profile};

#[tokio::test]
async fn publishes_an_independently_verified_complete_read_prefix() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/profile/default.json")).unwrap();
    assert_eq!(fixture.revision, "1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9");
    let expected = fixture
        .prefixes
        .iter()
        .find(|prefix| prefix.end == 23)
        .unwrap();
    let mut input = String::new();
    for (i, read) in fixture.reads.iter().take(23).enumerate() {
        writeln!(
            &mut input,
            "@read-{i}\n{read}\n+\n{}",
            "I".repeat(read.len())
        )
        .unwrap();
    }
    let database =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/profile/reference.syldb");
    let profiler = Profiler::open_syldb(database, ProfileOptions::default(), 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = Session::builder("synthetic-sample", profiler)
        .c(fixture.c)
        .k(fixture.k)
        .deduplication(if fixture.no_dedup {
            Deduplication::Disabled
        } else {
            Deduplication::Exact
        })
        .start()
        .unwrap();
    let router = session.receiver().router();
    let (stop, stopped) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let mut observations = Box::pin(session.observe());
    // Catch panics as well as timeouts so cleanup happens on failing assertions.
    let outcome = AssertUnwindSafe(timeout(Duration::from_secs(20), async {
        let receipt = Client::new(format!("http://{address}"), Limits::default())
            .send(Cursor::new(input.into_bytes()), |_| {})
            .await
            .unwrap();
        assert!(receipt.complete);
        loop {
            let observation = observations.next().await.unwrap();
            if observation
                .snapshot()
                .is_some_and(|snapshot| snapshot.checkpoint().reads() == 23)
            {
                break observation;
            }
        }
    }))
    .catch_unwind()
    .await;
    let cleanup = timeout(Duration::from_secs(10), async {
        let drained = session.shutdown().await;
        let _ = stop.send(());
        let transport_result = (&mut server).await;
        (drained, transport_result)
    })
    .await;
    if cleanup.is_err() {
        server.abort();
        let _ = timeout(Duration::from_secs(10), server).await;
    }
    let (drained, transport_result) = cleanup.expect("session and server must drain");
    drained.unwrap();
    transport_result.unwrap().unwrap();
    let observation = match outcome {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    };

    let snapshot = observation.snapshot().unwrap();
    // All 23 input records are 510 bases. These are processing totals, not marker counts.
    assert_eq!(snapshot.checkpoint().reads(), 23);
    assert_eq!(snapshot.checkpoint().bases(), 11_730);
    assert_eq!(observation.processed(), snapshot.checkpoint());
    assert_profile(snapshot.profile(), expected);
}

#[tokio::test]
async fn finish_returns_an_independent_terminal_profile_and_retains_finality_after_shutdown() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/profile/default.json")).unwrap();
    assert_eq!(fixture.revision, "1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9");
    let expected = fixture
        .prefixes
        .iter()
        .find(|prefix| prefix.end == 23)
        .unwrap();
    let mut input = String::new();
    for (i, read) in fixture.reads.iter().take(23).enumerate() {
        writeln!(
            &mut input,
            "@read-{i}\n{read}\n+\n{}",
            "I".repeat(read.len())
        )
        .unwrap();
    }
    let database =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/profile/reference.syldb");
    let profiler = Profiler::open_syldb(database, ProfileOptions::default(), 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = Session::builder("synthetic-sample", profiler)
        .c(fixture.c)
        .k(fixture.k)
        .deduplication(if fixture.no_dedup {
            Deduplication::Disabled
        } else {
            Deduplication::Exact
        })
        .start()
        .unwrap();
    let router = session.receiver().router();
    let (stop, stopped) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let outcome = AssertUnwindSafe(timeout(Duration::from_secs(20), async {
        let receipt = Client::new(format!("http://{address}"), Limits::default())
            .send(Cursor::new(input.into_bytes()), |_| {})
            .await
            .unwrap();
        assert!(receipt.complete);
        // Do not wait for periodic publication: finish must provide the terminal result.
        let snapshot = session.finish().await.unwrap();
        let observation = Box::pin(session.observe()).next().await.unwrap();
        (snapshot, observation)
    }))
    .catch_unwind()
    .await;
    let cleanup = timeout(Duration::from_secs(10), async {
        let drained = session.shutdown().await;
        let _ = stop.send(());
        let transport_result = (&mut server).await;
        (drained, transport_result)
    })
    .await;
    if cleanup.is_err() {
        server.abort();
        let _ = timeout(Duration::from_secs(10), server).await;
    }
    let (drained, transport_result) = cleanup.expect("session and server must drain");
    drained.unwrap();
    transport_result.unwrap().unwrap();
    let (snapshot, observation) = match outcome {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert_eq!(snapshot.checkpoint().reads(), 23);
    assert_eq!(snapshot.checkpoint().bases(), 11_730);
    assert_profile(snapshot.profile(), expected);

    let after_shutdown = timeout(Duration::from_secs(10), Box::pin(session.observe()).next())
        .await
        .expect("a fresh observer must receive retained finality after shutdown")
        .unwrap();
    for observation in [&observation, &after_shutdown] {
        assert_eq!(observation.processed(), snapshot.checkpoint());
        let terminal = observation
            .finish_outcome()
            .expect("successful finish must be recorded")
            .as_ref()
            .expect("recorded finish must be successful");
        assert_eq!(terminal.checkpoint().reads(), 23);
        assert_eq!(terminal.checkpoint().bases(), 11_730);
        assert_profile(terminal.profile(), expected);
    }
}

#[track_caller]
fn assert_profile(profile: &Profile, expected: &Prefix) {
    assert_eq!(profile.sample_name, "synthetic-sample");
    assert_eq!(profile.options, ProfileOptions::default());
    assert_eq!(profile.rows.len(), expected.rows.len());
    for expected in &expected.rows {
        let row = profile
            .rows
            .iter()
            .find(|row| {
                row.genome_name == expected["Genome_file"]
                    && row.contig_name == expected["Contig_name"]
            })
            .unwrap();
        for (name, actual, exponent) in [
            ("Adjusted_ANI", row.adjusted_ani.min(1.0) * 100.0, -2),
            ("Naive_ANI", row.naive_ani * 100.0, -2),
            ("Eff_cov", row.effective_coverage, -3),
            ("Mean_cov_geq1", row.mean_coverage, -3),
            ("Taxonomic_abundance", row.taxonomic_abundance_percent, -4),
        ] {
            let pinned: f64 = expected[name].parse().unwrap();
            let tolerance = 0.5_f64.mul_add(10_f64.powi(exponent), 1e-12);
            assert!(
                actual.is_finite() && (actual - pinned).abs() <= tolerance,
                "{name}: actual {actual}, pinned {pinned}"
            );
        }
        assert!(row.adjusted_ani.is_finite());
        assert_eq!(
            row.median_coverage,
            expected["Median_cov"].parse::<f64>().unwrap()
        );
        assert_eq!(
            format!("{}/{}", row.containment.0, row.containment.1),
            expected["Containment_ind"]
        );
        assert_eq!(
            row.kmers_reassigned.to_string(),
            expected["kmers_reassigned"]
        );
        let SequenceEstimate::Percent(abundance) = row.sequence_estimate else {
            panic!("relative-abundance profiling must report percentages");
        };
        assert_eq!(format!("{abundance:.4}"), expected["Sequence_abundance"]);
        let AdjustStatus::Lambda(lambda) = row.adjustment else {
            panic!("prefix-23 oracle has a numeric lambda");
        };
        assert_eq!(format!("{lambda:.3}"), expected["Eff_lambda"]);
        let (ani_low, ani_high) = row.ani_interval.unwrap();
        assert_eq!(
            format!("{:.2}-{:.2}", ani_low * 100.0, ani_high * 100.0),
            expected["ANI_5-95_percentile"]
        );
        let (lambda_low, lambda_high) = row.lambda_interval.unwrap();
        assert_eq!(
            format!("{lambda_low:.2}-{lambda_high:.2}"),
            expected["Lambda_5-95_percentile"]
        );
    }
}

#[tokio::test]
async fn events_publish_initial_state_then_an_independently_verified_profile() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/profile/default.json")).unwrap();
    assert_eq!(fixture.revision, "1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9");
    let expected = fixture
        .prefixes
        .iter()
        .find(|prefix| prefix.end == 23)
        .unwrap();
    let mut input = String::new();
    for (i, read) in fixture.reads.iter().take(23).enumerate() {
        writeln!(
            &mut input,
            "@read-{i}\n{read}\n+\n{}",
            "I".repeat(read.len())
        )
        .unwrap();
    }
    let database =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/profile/reference.syldb");
    let profiler = Profiler::open_syldb(database, ProfileOptions::default(), 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = Session::builder("synthetic-sample", profiler)
        .c(fixture.c)
        .k(fixture.k)
        .deduplication(if fixture.no_dedup {
            Deduplication::Disabled
        } else {
            Deduplication::Exact
        })
        .start()
        .unwrap();
    let router = session.router();
    let (stop, stopped) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let mut driver = None;
    let outcome = AssertUnwindSafe(timeout(Duration::from_secs(20), async {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await.unwrap();
        driver = Some(tokio::spawn(connection));
        let request = Request::builder()
            .uri(format!("http://{address}/events"))
            .header("accept", "text/event-stream")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_sse_journey(response, address, input, expected).await;
    }))
    .catch_unwind()
    .await;
    // Request sender and SSE body have dropped on success, timeout, and panic alike.
    let cleanup = AssertUnwindSafe(timeout(Duration::from_secs(10), async {
        let connection_result = if let Some(connection) = driver.as_mut() {
            if let Ok(result) = timeout(Duration::from_secs(2), &mut *connection).await {
                Some(result)
            } else {
                connection.abort();
                Some(timeout(Duration::from_secs(2), connection).await.unwrap())
            }
        } else {
            None
        };
        // A completed JoinHandle must not be polled again if later cleanup times out.
        driver = None;
        let drained = session.shutdown().await;
        let _ = stop.send(());
        let transport_result = (&mut server).await;
        (connection_result, drained, transport_result)
    }))
    .catch_unwind()
    .await;
    if !matches!(&cleanup, Ok(Ok(_))) {
        server.abort();
        if let Some(connection) = driver.as_mut() {
            connection.abort();
            let _ = timeout(Duration::from_secs(2), connection).await;
        }
        let _ = timeout(Duration::from_secs(2), &mut server).await;
    }
    // Report the original journey failure even if cleanup also panicked or timed out.
    match outcome {
        Ok(result) => result.expect("Flight-to-SSE tracer must complete within 20 seconds"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    let (connection_result, drained, transport_result) = match cleanup {
        Ok(result) => result.expect("SSE, session and server must drain"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    drained.unwrap();
    transport_result.unwrap().unwrap();
    if let Some(result) = connection_result {
        match result {
            Ok(result) => result.unwrap(),
            Err(error) => assert!(error.is_cancelled(), "HTTP driver panicked: {error}"),
        }
    }
}

#[tokio::test]
async fn http_finish_returns_and_retains_an_independently_verified_terminal_profile() {
    let fixture: Fixture = serde_json::from_str(include_str!("fixtures/profile/default.json"))
        .expect("pinned profile fixture must deserialize");
    assert_eq!(fixture.revision, "1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9");
    let expected = fixture
        .prefixes
        .iter()
        .find(|prefix| prefix.end == 23)
        .expect("independent fixture must contain the prefix-23 oracle");
    let mut input = String::new();
    for (i, read) in fixture.reads.iter().take(23).enumerate() {
        writeln!(
            &mut input,
            "@read-{i}\n{read}\n+\n{}",
            "I".repeat(read.len())
        )
        .expect("FASTQ fixture formatting must succeed");
    }
    let database =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/profile/reference.syldb");
    let profiler = Profiler::open_syldb(database, ProfileOptions::default(), 1)
        .expect("pinned reference database must open");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("HTTP finish tracer must bind a loopback listener");
    let address = listener.local_addr().expect("bound listener address");
    let session = Session::builder("synthetic-sample", profiler)
        .c(fixture.c)
        .k(fixture.k)
        .deduplication(if fixture.no_dedup {
            Deduplication::Disabled
        } else {
            Deduplication::Exact
        })
        .refresh_interval(Duration::from_secs(3600))
        .start()
        .expect("fixture profiling session must start");
    let router = session.router();
    let (stop, stopped) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let mut driver = None;
    let outcome = AssertUnwindSafe(timeout(Duration::from_secs(20), async {
        assert_http_finish_journey(&mut driver, address, input, expected).await;
    }))
    .catch_unwind()
    .await;
    let cleanup = AssertUnwindSafe(timeout(Duration::from_secs(10), async {
        let connection_result = if let Some(connection) = driver.as_mut() {
            if let Ok(result) = timeout(Duration::from_secs(2), &mut *connection).await {
                Some(result)
            } else {
                connection.abort();
                Some(
                    timeout(Duration::from_secs(2), connection)
                        .await
                        .expect("aborted HTTP driver must terminate"),
                )
            }
        } else {
            None
        };
        driver = None;
        let drained = session.shutdown().await;
        let _ = stop.send(());
        let transport_result = (&mut server).await;
        (connection_result, drained, transport_result)
    }))
    .catch_unwind()
    .await;
    if !matches!(&cleanup, Ok(Ok(_))) {
        server.abort();
        if let Some(connection) = driver.as_mut() {
            connection.abort();
            let _ = timeout(Duration::from_secs(2), connection).await;
        }
        let _ = timeout(Duration::from_secs(2), &mut server).await;
    }
    // Preserve the journey's original assertion failure even if cleanup also fails.
    match outcome {
        Ok(result) => result.expect("HTTP finish tracer must complete within 20 seconds"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    let (connection_result, drained, transport_result) = match cleanup {
        Ok(result) => result.expect("HTTP driver, session and server must drain within 10 seconds"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    drained.expect("finished session must shut down cleanly");
    transport_result
        .expect("HTTP server task must not panic")
        .expect("HTTP server must shut down cleanly");
    if let Some(result) = connection_result {
        match result {
            Ok(result) => result.expect("HTTP driver must close cleanly"),
            Err(error) => assert!(error.is_cancelled(), "HTTP driver panicked: {error}"),
        }
    }
}

async fn assert_invalid_finish_requests(
    sender: &mut http1::SendRequest<Full<Bytes>>,
    address: SocketAddr,
) {
    for (body, content_type) in [
        ("{", Some("application/json")),
        ("", Some("application/json")),
        ("null", Some("application/json")),
        ("[]", Some("application/json")),
        ("{\"unexpected\":true}", Some("application/json")),
        ("{}", None),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/finish"));
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = sender
            .send_request(
                request
                    .body(Full::new(Bytes::from_static(body.as_bytes())))
                    .expect("invalid finish request must build"),
            )
            .await
            .expect("invalid finish request must receive a response");
        assert!(
            response.status().is_client_error(),
            "invalid finish input accepted: {body:?}"
        );
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("invalid finish response must be readable")
            .to_bytes();
        let error: Value = serde_json::from_slice(&bytes)
            .expect("invalid finish response must use the JSON error envelope");
        assert_eq!(error["error"]["code"], "invalid_finish_request");
        assert_ne!(
            error["error"]["message"]
                .as_str()
                .expect("invalid finish error must have a message"),
            ""
        );
    }
}

async fn assert_http_finish_journey(
    driver: &mut Option<JoinHandle<Result<(), hyper::Error>>>,
    address: SocketAddr,
    input: String,
    expected: &Prefix,
) {
    let stream = TcpStream::connect(address)
        .await
        .expect("HTTP client must connect to the session listener");
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1 handshake must succeed");
    *driver = Some(tokio::spawn(connection));
    assert_invalid_finish_requests(&mut sender, address).await;
    // Rejected requests must not close ingestion: a real upload must still succeed.
    let receipt = Client::new(format!("http://{address}"), Limits::default())
        .send(Cursor::new(input.into_bytes()), |_| {})
        .await
        .expect(
            "real Flight upload must accept the 23 fixture reads after invalid finish requests",
        );
    assert!(receipt.complete);
    let checkpoint = json!({"reads": "23", "bases": "11730"});
    let terminal_status = json!({
        "sample_name": "synthetic-sample", "state": "stopped",
        "processed": checkpoint, "profile_checkpoint": checkpoint,
        "ingestion_error": null, "refresh_error": null,
        "finish": {"status": "succeeded", "checkpoint": checkpoint}
    });
    let mut retained = None;
    // Finish must produce the terminal result without waiting for a periodic refresh.
    for _ in 0..2 {
        let request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/finish"))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from_static(b"{}")))
            .expect("JSON finish request must build");
        let response = sender
            .send_request(request)
            .await
            .expect("HTTP finish request must receive a response");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "POST /finish must succeed"
        );
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("complete finish JSON body must be readable")
            .to_bytes();
        let snapshot: Value =
            serde_json::from_slice(&bytes).expect("finish response must be a full JSON snapshot");
        assert_eq!(snapshot["status"], terminal_status);
        assert_eq!(snapshot["profile"]["checkpoint"], checkpoint);
        assert_wire_profile(&snapshot["profile"], expected);
        if let Some(previous) = &retained {
            assert_eq!(
                &snapshot, previous,
                "repeated finish must retain the same result"
            );
        }
        retained = Some(snapshot);
    }
    let request = Request::builder()
        .uri(format!("http://{address}/events"))
        .header("accept", "text/event-stream")
        .body(Full::new(Bytes::new()))
        .expect("terminal SSE request must build");
    let response = sender
        .send_request(request)
        .await
        .expect("listener must still serve events after repeated finish");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut events = SseReader {
        body: response.into_body(),
        pending: Vec::new(),
    };
    let (name, terminal) = events.next().await;
    assert_eq!(name, "snapshot");
    assert_eq!(Some(&terminal), retained.as_ref());
}

async fn assert_sse_journey(
    response: hyper::Response<Incoming>,
    address: SocketAddr,
    input: String,
    expected: &Prefix,
) {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.version(), hyper::Version::HTTP_11);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut events = SseReader {
        body: response.into_body(),
        pending: Vec::new(),
    };
    let (name, initial) = events.next().await;
    assert_eq!(name, "snapshot");
    assert_eq!(initial.get("profile"), Some(&Value::Null));
    assert_eq!(
        initial["status"],
        json!({
            "sample_name": "synthetic-sample", "state": "open",
            "processed": {"reads": "0", "bases": "0"}, "profile_checkpoint": null,
            "ingestion_error": null, "refresh_error": null, "finish": null
        })
    );

    // The SSE initial state is received before any Flight upload on this same listener.
    let receipt = Client::new(format!("http://{address}"), Limits::default())
        .send(Cursor::new(input.into_bytes()), |_| {})
        .await
        .unwrap();
    assert!(receipt.complete);
    for _ in 0..128 {
        let (name, snapshot) = events.next().await;
        assert!(
            name == "status" || name == "snapshot",
            "unexpected event: {name}"
        );
        if name == "status" || snapshot["profile"].is_null() {
            continue;
        }
        let profile = &snapshot["profile"];
        if profile["checkpoint"]["reads"] != "23" {
            continue;
        }
        let checkpoint = json!({"reads": "23", "bases": "11730"});
        assert_eq!(profile["checkpoint"], checkpoint);
        assert_eq!(
            snapshot["status"],
            json!({
                "sample_name": "synthetic-sample", "state": "open",
                "processed": checkpoint, "profile_checkpoint": checkpoint,
                "ingestion_error": null, "refresh_error": null, "finish": null
            })
        );
        assert_wire_profile(profile, expected);
        return;
    }
    panic!("no prefix-23 snapshot within 128 SSE events");
}

struct SseReader {
    body: Incoming,
    pending: Vec<u8>,
}

impl SseReader {
    async fn next(&mut self) -> (String, Value) {
        let mut event = String::from("message");
        let mut data = String::new();
        // Bound both incoming frames and line parsing; a body frame is not an SSE event.
        for _ in 0..8192 {
            if let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                let bytes: Vec<_> = self.pending.drain(..=end).collect();
                let line = std::str::from_utf8(&bytes)
                    .unwrap()
                    .trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    if !data.is_empty() {
                        return (
                            event,
                            serde_json::from_str(&data).expect("SSE data must be JSON"),
                        );
                    }
                    event = String::from("message");
                    continue;
                }
                if line.starts_with(':') {
                    continue;
                }
                let (field, value) = line.split_once(':').unwrap_or((line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => event = value.to_owned(),
                    "data" => {
                        data.push_str(value);
                        data.push('\n');
                    }
                    _ => {}
                }
                assert!(
                    data.len() <= 1_048_576,
                    "SSE event exceeds test's 1 MiB limit"
                );
            } else {
                let frame = self
                    .body
                    .frame()
                    .await
                    .expect("SSE ended before expected event")
                    .unwrap();
                if let Ok(bytes) = frame.into_data() {
                    self.pending.extend_from_slice(&bytes);
                    assert!(
                        self.pending.len() <= 1_048_576,
                        "SSE buffer exceeds test's 1 MiB limit"
                    );
                }
            }
        }
        panic!("SSE event exceeded bounded frame/line budget");
    }
}
