//! Read transfer through the same client and server interfaces used by applications.

use std::{
    collections::HashMap,
    io::{self, Cursor, Read, Write},
    sync::{Arc, mpsc},
    time::Duration,
};

use arrow_array::{BinaryArray, RecordBatch};
use arrow_flight::{FlightClient, encode::FlightDataEncoderBuilder};
use arrow_schema::{DataType, Field, Schema};
use axum::{Router, body::Body, http::Response, routing::post};
use axylph::{Client, Error, Limits, Progress, Receiver};
use flate2::{Compression, write::GzEncoder};
use futures_util::{StreamExt, stream};
use sylph::read_sketch::{Deduplication, ReadSketcher, SketchError, SketchParams};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tonic::{Code, transport::Endpoint};

fn receiver(c: usize) -> Result<Receiver, SketchError> {
    Ok(Receiver::new(
        Limits::default(),
        ReadSketcher::new(
            SketchParams {
                c,
                k: 31,
                deduplication: Deduplication::Exact,
            },
            "transfer".to_owned(),
            None,
        )?,
    ))
}

struct GatedInput {
    prefix: Cursor<Vec<u8>>,
    blocked: Option<oneshot::Sender<()>>,
    release: mpsc::Receiver<()>,
    resumed: Cursor<Vec<u8>>,
    suffix: Cursor<Vec<u8>>,
    consumed_suffix: bool,
    dropped: Option<oneshot::Sender<bool>>,
}

impl Read for GatedInput {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let count = self.prefix.read(output)?;
        if count != 0 {
            return Ok(count);
        }
        if let Some(blocked) = self.blocked.take() {
            let _ = blocked.send(());
            // Dropping the sender also releases this gate on panic or timeout.
            if matches!(
                self.release.recv_timeout(Duration::from_secs(20)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "input gate"));
            }
        }
        let count = self.resumed.read(output)?;
        if count != 0 {
            return Ok(count);
        }
        let count = self.suffix.read(output)?;
        self.consumed_suffix |= count != 0;
        Ok(count)
    }
}

impl Drop for GatedInput {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(self.consumed_suffix);
        }
    }
}

#[tokio::test]
async fn dropping_send_releases_admission_before_blocked_input_and_preserves_prefix() {
    let receiver = receiver(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = receiver.clone().router();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    // Supply 8 KiB of complete records so buffered parsing can acknowledge a
    // nonempty prefix before input stalls, without requiring an artificial EOF.
    let sequence: String = "CTATGTCAAGCGGAACTATCTGGCGCTCAATCTTGCTAATCAAAAAAAAAAAAAAAAAAAAAAAA"
        .chars()
        .cycle()
        .take(123)
        .collect();
    let record = format!("@read\n{sequence}\n+\n{}\n", "I".repeat(sequence.len())).into_bytes();
    let (blocked, blocked_input) = oneshot::channel();
    let (release, gate) = mpsc::channel();
    let (dropped, dropped_input) = oneshot::channel();
    let input = GatedInput {
        prefix: Cursor::new(record.repeat(32)),
        blocked: Some(blocked),
        release: gate,
        resumed: Cursor::new(record.clone()),
        suffix: Cursor::new(record),
        consumed_suffix: false,
        dropped: Some(dropped),
    };
    let client = Client::new(format!("http://{address}"), Limits::new(1024, 1).unwrap());
    let (receipt, accepted) = oneshot::channel();
    let mut receipt = Some(receipt);
    let mut upload = Box::pin(client.send(input, move |progress| {
        if !progress.complete
            && let Some(receipt) = receipt.take()
        {
            let _ = receipt.send(progress);
        }
    }));
    let exercise = async {
        let ((), progress) = timeout(Duration::from_secs(5), async {
            tokio::select! {
                ready = async { tokio::try_join!(blocked_input, accepted) } => {
                    ready.map_err(io::Error::other)
                }
                result = &mut upload => {
                    Err(io::Error::other(format!("upload ended before cancellation: {result:?}")))
                }
            }
        })
        .await??;
        let prefix = receiver
            .inspect(|s| (s.kmer_counts.clone(), s.mean_read_length.to_bits()))
            .await?;
        // Drop the owned future, not merely a pinned reference or Flight handle.
        drop(upload);
        let admission = timeout(Duration::from_secs(2), async {
            loop {
                match client.send(Cursor::new(Vec::<u8>::new()), |_| {}).await {
                    Err(Error::Flight(arrow_flight::error::FlightError::Tonic(status)))
                        if status.code() == Code::ResourceExhausted => {}
                    result => break result,
                }
            }
        })
        .await;
        Ok::<_, Box<dyn std::error::Error>>((progress, prefix, admission))
    }
    .await;

    // Release the finite remainder before asserting so failures cannot strand
    // a blocking parser task during runtime shutdown.
    drop(release);
    let dropped = timeout(Duration::from_secs(5), dropped_input).await;
    let after = timeout(
        Duration::from_secs(5),
        receiver.inspect(|s| (s.kmer_counts.clone(), s.mean_read_length.to_bits())),
    )
    .await;
    server.abort();
    let _ = server.await;

    let (progress, prefix, admission) = exercise.unwrap();
    assert!(progress.reads > 0);
    assert!(
        !prefix.0.is_empty(),
        "the accepted prefix must contain markers"
    );
    // Check source cleanup before asserting request cancellation.
    let consumed_suffix = dropped
        .expect("source was not dropped after release")
        .unwrap();
    assert_eq!(
        admission
            .expect("cancelled upload kept admission while Read was blocked")
            .unwrap(),
        Progress {
            complete: true,
            ..Progress::default()
        }
    );
    assert!(
        !consumed_suffix,
        "cancelled parser consumed input beyond the in-flight Read"
    );
    let after = after.unwrap().unwrap();
    // Already-dispatched batches may finish; accepted markers must not vanish.
    for (marker, count) in prefix.0 {
        assert!(after.0.get(&marker).is_some_and(|&total| total >= count));
    }
    assert_eq!(after.1, prefix.1);
}

#[tokio::test]
async fn reports_validated_reads_from_plain_and_gzip_fastq() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, receiver(200).unwrap().router())
            .await
            .unwrap();
    });
    let reads = b"@one\nACGT\n+\nIIII\n@two\nGGA\n+\n!!!\n";
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(reads).unwrap();
    for input in [reads.to_vec(), gzip.finish().unwrap()] {
        let mut updates = Vec::new();
        let result = Client::new(format!("http://{address}"), Limits::new(1024, 1).unwrap())
            .send(Cursor::new(input), |progress| updates.push(progress))
            .await
            .unwrap();
        assert_eq!(
            updates,
            vec![
                Progress {
                    reads: 1,
                    bases: 4,
                    complete: false
                },
                Progress {
                    reads: 2,
                    bases: 7,
                    complete: false
                },
                Progress {
                    reads: 2,
                    bases: 7,
                    complete: true
                },
            ]
        );
        assert_eq!(updates.last(), Some(&result));
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn rejects_non_nucleotide_data_at_the_flight_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, receiver(200).unwrap().router())
            .await
            .unwrap();
    });
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Binary, false),
            Field::new("sequence", DataType::Binary, false),
            Field::new("quality", DataType::Binary, true),
        ],
        HashMap::from([("axylph.version".to_owned(), "0".to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"one"])),
            Arc::new(BinaryArray::from_vec(vec![b"ACGZ"])),
            Arc::new(BinaryArray::from_vec(vec![b"IIII"])),
        ],
    )
    .unwrap();
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    let mut responses = client
        .do_put(FlightDataEncoderBuilder::new().build(stream::iter([Ok(batch)])))
        .await
        .unwrap();
    let error = responses.next().await.unwrap().unwrap_err();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), Code::InvalidArgument);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn accepts_a_read_near_the_batch_byte_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, receiver(200).unwrap().router())
            .await
            .unwrap();
    });
    // 1 identifier byte + 524287 bases + 524287 qualities = 1048575 data bytes.
    let input = format!("@r\n{}\n+\n{}\n", "A".repeat(524_287), "I".repeat(524_287));
    let result = Client::new(format!("http://{address}"), Limits::default())
        .send(Cursor::new(input), |_| {})
        .await
        .unwrap();
    assert_eq!(
        result,
        Progress {
            reads: 1,
            bases: 524_287,
            complete: true
        }
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn does_not_trust_an_early_completion_from_a_peer() {
    // This peer ignores the upload and claims success with zero validated reads.
    // Build its small protobuf/gRPC response independently of the client codec.
    let app = Router::new().route(
        "/arrow.flight.protocol.FlightService/DoPut",
        post(|| async {
            let json = br#"{"reads":0,"bases":0,"complete":true}"#;
            let mut protobuf = vec![10, u8::try_from(json.len()).unwrap()];
            protobuf.extend_from_slice(json);
            let mut wire = vec![0];
            wire.extend_from_slice(&u32::try_from(protobuf.len()).unwrap().to_be_bytes());
            wire.extend_from_slice(&protobuf);
            Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(Body::from(wire))
                .unwrap()
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for input in [b"@one\nACGT\n+\nIIII\n".as_slice(), b"@broken\nACGT\n"] {
        let mut completed = false;
        let result = Client::new(format!("http://{address}"), Limits::default())
            .send(Cursor::new(input), |progress| {
                completed |= progress.complete;
            })
            .await;
        assert!(
            result.is_err(),
            "a peer must not hide unread or invalid input"
        );
        assert!(!completed);
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn preserves_local_input_errors_through_flight() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, receiver(200).unwrap().router())
            .await
            .unwrap();
    });
    let client = Client::new(format!("http://{address}"), Limits::new(10, 1).unwrap());
    let error = client
        .send(Cursor::new(b"@broken\nACGT\n"), |progress| {
            assert!(!progress.complete);
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Fastq(_)), "{error:?}");

    let error = client
        .send(Cursor::new(b"@long\nACGT\n+\nIIII\n"), |progress| {
            assert!(!progress.complete);
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::ReadTooLarge {
                bytes: 12,
                limit: 10
            }
        ),
        "{error:?}"
    );
    server.abort();
    let _ = server.await;
}
