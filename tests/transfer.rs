//! Read transfer through the same client and server interfaces used by applications.

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use arrow_array::{BinaryArray, RecordBatch};
use arrow_flight::FlightClient;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_schema::{DataType, Field, Schema};
use axum::{Router, body::Body, http::Response, routing::post};
use axylph::{Client, Limits, Progress, Receiver};
use flate2::{Compression, write::GzEncoder};
use futures_util::{StreamExt, stream};
use tokio::net::TcpListener;
use tonic::{Code, transport::Endpoint};

#[tokio::test]
async fn reports_validated_reads_from_plain_and_gzip_fastq() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Receiver::new(Limits::default()).router())
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
        axum::serve(listener, Receiver::new(Limits::default()).router())
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
        axum::serve(listener, Receiver::new(Limits::default()).router())
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
