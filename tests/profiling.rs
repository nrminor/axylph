//! Real Flight input and periodic and terminal profiles against an independent oracle.
//!
//! `default.json` is a lossless YAML-to-JSON conversion of the sylph fork's pinned
//! `default.yaml` (SHA-256 8af09a9fbabe124676d25ae9d00a24e0e59bde756b28d16ab506b37617fef966).
//! The reference database is copied byte-for-byte. Neither oracle was regenerated.

#![cfg(test)]

use std::{
    collections::BTreeMap, fmt::Write, io::Cursor, panic::AssertUnwindSafe, path::Path,
    time::Duration,
};

use axylph::{Client, Limits, Session};
use futures_util::{FutureExt, StreamExt};
use serde::Deserialize;
use sylph::{
    profile::{Profile, ProfileOptions, Profiler, SequenceEstimate},
    read_sketch::Deduplication,
    types::AdjustStatus,
};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

#[derive(Deserialize)]
struct Fixture {
    revision: String,
    c: usize,
    k: usize,
    no_dedup: bool,
    reads: Vec<String>,
    prefixes: Vec<Prefix>,
}

#[derive(Deserialize)]
struct Prefix {
    end: usize,
    rows: Vec<BTreeMap<String, String>>,
}

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
