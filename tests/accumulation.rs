//! Cumulative sketches observed through real Flight uploads and the receiver API.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use arrow_array::{BinaryArray, RecordBatch};
use arrow_flight::{FlightClient, encode::FlightDataEncoderBuilder, error::FlightError};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use axylph::{Error, Limits, Progress, Receiver};
use futures_util::{StreamExt, stream};
use serde::Deserialize;
use sylph::{
    read_sketch::{Deduplication, ReadSketcher, SketchError, SketchParams},
    types::SequencesSketch,
};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Code, Status,
    transport::{Endpoint, Error as TransportError},
};

#[derive(Deserialize)]
struct Fixtures {
    revision: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    c: usize,
    k: usize,
    no_dedup: bool,
    reads: Vec<String>,
    prefixes: Vec<Expected>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Expected {
    kmer_counts: BTreeMap<u64, u32>,
    c: usize,
    k: usize,
    file_name: String,
    sample_name: Option<String>,
    paired: bool,
    mean_read_length_bits: u64,
}

fn observe(sketch: &SequencesSketch) -> Expected {
    Expected {
        kmer_counts: sketch.kmer_counts.iter().map(|(&k, &v)| (k, v)).collect(),
        c: sketch.c,
        k: sketch.k,
        file_name: sketch.file_name.clone(),
        sample_name: sketch.sample_name.clone(),
        paired: sketch.paired,
        mean_read_length_bits: sketch.mean_read_length.to_bits(),
    }
}

fn receiver(case: &Case) -> Result<Receiver, SketchError> {
    let sketcher = ReadSketcher::new(
        SketchParams {
            c: case.c,
            k: case.k,
            deduplication: if case.no_dedup {
                Deduplication::Disabled
            } else {
                Deduplication::Exact
            },
        },
        "reads.fastq".to_owned(),
        Some("fixture".to_owned()),
    )?;
    Ok(Receiver::new(Limits::default(), sketcher))
}

fn batch(reads: &[String]) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Binary, false),
            Field::new("sequence", DataType::Binary, false),
            Field::new("quality", DataType::Binary, true),
        ],
        [("axylph.version".to_owned(), "0".to_owned())],
    ));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from_iter_values(reads.iter().map(|_| b"read"))),
            Arc::new(BinaryArray::from_iter_values(
                reads.iter().map(String::as_bytes),
            )),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>; reads.len()])),
        ],
    )
}

async fn connect(address: SocketAddr) -> Result<FlightClient, TransportError> {
    Ok(FlightClient::new(
        Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?,
    ))
}

async fn upload(address: SocketAddr, batch: RecordBatch) -> Result<Vec<Progress>, Error> {
    let mut responses = connect(address)
        .await?
        .do_put(FlightDataEncoderBuilder::new().build(stream::iter([Ok(batch)])))
        .await?;
    let mut receipts = Vec::new();
    while let Some(response) = responses.next().await {
        receipts.push(serde_json::from_slice(&response?.app_metadata)?);
    }
    Ok(receipts)
}

async fn wait_until_idle(address: SocketAddr) -> Result<Vec<Progress>, Error> {
    // Flight encodes an empty batch as a schema-only upload. Retry only admission
    // failures while the disconnected request is being cleaned up.
    loop {
        match upload(address, batch(&[])?).await {
            Err(Error::Flight(FlightError::Tonic(status)))
                if status.code() == Code::ResourceExhausted => {}
            result => return result,
        }
    }
}

#[tokio::test]
async fn preserves_sketch_state_and_running_mean_across_uploads() {
    timeout(Duration::from_secs(20), async {
        let fixtures: Fixtures =
            serde_json::from_str(include_str!("fixtures/read-sketch.json")).unwrap();
        assert_eq!(
            fixtures.revision,
            "1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9"
        );
        assert_eq!(fixtures.cases.len(), 2);
        assert!(fixtures.cases.iter().any(|case| case.no_dedup));
        assert!(fixtures.cases.iter().any(|case| !case.no_dedup));
        for case in fixtures.cases {
            let receiver = receiver(&case).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = receiver.clone().router();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            assert!(
                receiver
                    .inspect(observe)
                    .await
                    .unwrap()
                    .kmer_counts
                    .is_empty()
            );

            for (range, prefix, reads, bases) in
                [(0..2, 1, 2, 85), (2..4, 3, 2, 131), (4..5, 4, 1, 66)]
            {
                let accepted = Progress {
                    reads,
                    bases,
                    complete: false,
                };
                assert_eq!(
                    upload(address, batch(&case.reads[range]).unwrap())
                        .await
                        .unwrap(),
                    [
                        accepted,
                        Progress {
                            complete: true,
                            ..accepted
                        }
                    ]
                );
                for _ in 0..2 {
                    assert_eq!(
                        receiver.inspect(observe).await.unwrap(),
                        case.prefixes[prefix]
                    );
                }
            }
            server.abort();
            let _ = server.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn preserves_accepted_prefix_and_releases_upload_after_disconnect() {
    timeout(Duration::from_secs(20), async {
        let fixtures: Fixtures =
            serde_json::from_str(include_str!("fixtures/read-sketch.json")).unwrap();
        let case = fixtures
            .cases
            .into_iter()
            .find(|case| !case.no_dedup)
            .unwrap();
        let receiver = receiver(&case).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = receiver.clone().router();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // A one-connection relay lets this test sever the actual TCP transport,
        // rather than assuming that dropping a response also cancels its sender.
        let relay = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_address = relay.local_addr().unwrap();
        let connection = tokio::spawn(async move {
            let (mut downstream, _) = relay.accept().await.unwrap();
            let mut upstream = TcpStream::connect(address).await.unwrap();
            copy_bidirectional(&mut downstream, &mut upstream)
                .await
                .unwrap();
        });
        let (sender, input) = mpsc::channel(1);
        sender
            .send(Ok(batch(&case.reads[..4]).unwrap()))
            .await
            .unwrap();
        let mut client = connect(relay_address).await.unwrap();
        let mut responses = client
            .do_put(FlightDataEncoderBuilder::new().build(ReceiverStream::new(input)))
            .await
            .unwrap();
        let response = responses.next().await.unwrap().unwrap();
        let progress: Progress = serde_json::from_slice(&response.app_metadata).unwrap();
        assert_eq!(
            progress,
            Progress {
                reads: 4,
                bases: 216,
                complete: false
            }
        );
        assert_eq!(receiver.inspect(observe).await.unwrap(), case.prefixes[3]);

        let mut competing = connect(address).await.unwrap();
        let error = competing
            .do_put(
                FlightDataEncoderBuilder::new()
                    .build(stream::iter([Ok(batch(&case.reads[4..]).unwrap())])),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(Status::from(error).code(), Code::ResourceExhausted);

        connection.abort();
        assert!(connection.await.unwrap_err().is_cancelled());
        // Keep the sender alive: no clean request EOF precedes the disconnect.
        assert!(responses.next().await.unwrap().is_err());
        sender.closed().await;
        drop(responses);
        drop(client);

        assert_eq!(
            wait_until_idle(address).await.unwrap(),
            [Progress {
                complete: true,
                ..Progress::default()
            }]
        );
        drop(sender);
        assert_eq!(receiver.inspect(observe).await.unwrap(), case.prefixes[3]);

        let accepted = Progress {
            reads: 1,
            bases: 66,
            complete: false,
        };
        assert_eq!(
            upload(address, batch(&case.reads[4..]).unwrap())
                .await
                .unwrap(),
            [
                accepted,
                Progress {
                    complete: true,
                    ..accepted
                }
            ]
        );
        assert_eq!(receiver.inspect(observe).await.unwrap(), case.prefixes[4]);
        server.abort();
        let _ = server.await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejects_an_invalid_batch_without_mutating_the_accepted_prefix() {
    timeout(Duration::from_secs(20), async {
        let fixtures: Fixtures =
            serde_json::from_str(include_str!("fixtures/read-sketch.json")).unwrap();
        let case = fixtures
            .cases
            .into_iter()
            .find(|case| !case.no_dedup)
            .unwrap();
        let receiver = receiver(&case).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = receiver.clone().router();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = connect(address).await.unwrap();
        let mut responses = client
            .do_put(FlightDataEncoderBuilder::new().build(stream::iter([
                Ok(batch(&case.reads[..1]).unwrap()),
                Ok(batch(&[case.reads[1].clone(), "ACGZ".to_owned()]).unwrap()),
            ])))
            .await
            .unwrap();
        let response = responses.next().await.unwrap().unwrap();
        let progress: Progress = serde_json::from_slice(&response.app_metadata).unwrap();
        assert_eq!(
            progress,
            Progress {
                reads: 1,
                bases: 20,
                complete: false
            }
        );
        let error = responses.next().await.unwrap().unwrap_err();
        assert_eq!(Status::from(error).code(), Code::InvalidArgument);
        assert!(responses.next().await.is_none());
        assert_eq!(receiver.inspect(observe).await.unwrap(), case.prefixes[0]);

        let accepted = Progress {
            reads: 1,
            bases: 65,
            complete: false,
        };
        assert_eq!(
            upload(address, batch(&case.reads[1..2]).unwrap())
                .await
                .unwrap(),
            [
                accepted,
                Progress {
                    complete: true,
                    ..accepted
                }
            ]
        );
        assert_eq!(receiver.inspect(observe).await.unwrap(), case.prefixes[1]);
        server.abort();
        let _ = server.await;
    })
    .await
    .unwrap();
}
