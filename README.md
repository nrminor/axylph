# Axylph: incremental metagenomic profiling on the backend

Axylph is a streaming metagenomic profiler meant to be deployed as a service on your backend infrastructure. It wraps the [Sylph metagenomic profiler](https://github.com/bluenote-1577/sylph), built as a library, in HTTP endpoints with [Axum](https://github.com/tokio-rs/axum) (Axylph is a portmanteau of Axum and Sylph).

Axylph bundles a client and a server. Its default mode is a server mode, which waits for sequencing reads as they arrive on the configured address and port. It also comes with a client mode, which can be run where sequencing reads are published to send them over the wire to the server using [Arrow Flight protocol](https://arrow.apache.org/docs/format/Flight.html).

Unlike the Sylph command line app, Axylph supports incremental profiling, where the profile updates as new reads stream in. This makes it possible to build realtime dashboards showing the taxonomic breakdown of a sample, for example, as reads are published by an Oxford Nanopore instrument. Note that this is only practical because of the excellent performance of Sylph!

## Start profiling

Start a server with a Sylph reference database. It keeps the reference loaded and refreshes the sample's profile as reads arrive:

```sh
# This example assumes a reference with k=31 and minimum c=50.
axylph serve \
  --sample run-001 \
  --reference /data/references/reference.syldb \
  --c 50 \
  --k 31
```

The terminal HUD shows connection details, live read/base counts, and profile status. The server listens on `127.0.0.1:50051` by default; add `--listen 0.0.0.0:50051` to accept clients from other machines. Use a trusted network: authentication and TLS must be provided outside Axylph.

Match `k` to your reference and choose a positive `c` no greater than the reference's minimum `c`. Running just `axylph` starts a sketch-only server; adding `--reference` enables profiling.

Currently, each Axylph instance assumes it's receiving reads for a single sample-level entity. This may change in the future.

## Send reads as they arrive

In another terminal, use the same executable as a client. Send completed FASTQ files sequentially, either plain or gzip-compressed:

```sh
axylph send reads-001.fastq.gz
axylph send reads-002.fastq.gz
```

Each upload adds to the same sample. You can run the client alongside your sequencing instrument and the server on a separate backend:

```sh
axylph send reads-003.fastq.gz --server http://sequencer-backend:50051
```

## Follow the profile

To see the updating profile, open another terminal and subscribe to the running server. For example, with curl:

```sh
curl --no-buffer http://127.0.0.1:50051/events
```

Leave this running while the Axylph client sends FASTQ files. It prints live Server-Sent Events: `snapshot` events contain the cumulative profile in `profile.rows`, and `status` events report processing progress. Each connection starts with the current snapshot; `profile` is `null` until the first successful calculation. Subsequent snapshots contain the complete updated profile, so you can follow how the results change as more reads arrive.

The server exposes all three interfaces on the same address and port:

| Interface | Purpose |
| --- | --- |
| Arrow Flight `DoPut` | Receive read batches from `axylph send`. |
| `GET /events` | Stream status and cumulative profiles as Server-Sent Events. |
| `POST /finish` | Return the final profile, used by `axylph finish`. |

The observation and finish endpoints require profiling to be enabled.

## Save the result

Axylph runs indefinitely by default unless you tell it to shut down. To do so, finish the sample and save its final profile as JSON with:

```sh
axylph finish > result.json

# For a remote server:
axylph finish --server http://sequencer-backend:50051 > result.json
```

This ends uploads for the sample. Once the result is saved, stop the server with Ctrl-C. State currently lives in memory, so save before stopping.

## CLI at a glance

The same executable runs both sides of the connection:

```sh
axylph                       # Start the server using configured settings.
axylph serve                 # Explicit server mode; equivalent to the above.
axylph send reads.fastq.gz    # Add reads to the sample.
axylph finish > result.json  # Finish the sample and save its profile.
axylph schema > axylph.schema.json

axylph serve --help
axylph send --help
axylph finish --help
```

## Configure a run

Keep reusable settings for the server and clients in `axylph.toml`. All settings are optional; this example uses a reference with `k=31` and minimum `c=50`:

```toml
# axylph.toml
#:schema ./axylph.schema.json
batch_bytes = 1048576
batch_reads = 1024

[serve]
sample = "run-001"
listen = "127.0.0.1:50051"
reference = "references/reference.syldb"
c = 50
k = 31
dedup = true
profile_workers = 2
refresh_interval_ms = 1000

[send]
server = "http://127.0.0.1:50051"

[finish]
server = "http://127.0.0.1:50051"
```

`profile_workers` controls the profiling worker pool; `refresh_interval_ms` sets the minimum spacing between calculations. The root batch limits apply to both uploads and the receiver: combined identifier, sequence, and quality bytes, and reads per batch.

```sh
# Use the same file for the server and its clients.
axylph --config axylph.toml --sample run-001
axylph send reads.fastq.gz --config axylph.toml
axylph finish --config axylph.toml > result.json

# Override the configured sample for a new server process.
AXYLPH_SAMPLE=run-002 axylph --config axylph.toml

# Configure profiling through the process environment.
AXYLPH_REFERENCE=/data/references/reference.syldb \
AXYLPH_C=50 \
AXYLPH_K=31 \
AXYLPH_PROFILE_WORKERS=2 \
axylph

# Point either client at a remote server.
AXYLPH_SERVER=http://sequencer-backend:50051 axylph send reads.fastq.gz
AXYLPH_SERVER=http://sequencer-backend:50051 axylph finish > result.json
```

Command-line arguments override `AXYLPH_*` environment variables, which override TOML settings and defaults. Without `--config`, Axylph uses `AXYLPH_CONFIG` or looks for `axylph.toml` in the invocation directory. Relative reference paths in TOML are resolved from that file's directory; argument and environment paths are relative to the invocation directory.

Other environment overrides are `AXYLPH_LISTEN`, `AXYLPH_REFRESH_INTERVAL_MS`, `AXYLPH_DEDUP` (`true` or `false`), `AXYLPH_BATCH_BYTES`, `AXYLPH_BATCH_READS`, and `AXYLPH_DISPLAY` (the root TOML `display` setting). Axylph reads the process environment rather than loading `.env` files. Generate the schema with `axylph schema > axylph.schema.json` to enable editor validation using the comment above.

## Roadmap

The next two priorities are:

1. **Automatically send new FASTQ files.** Watch instrument or basecaller output directories, identify completed files, and queue them for upload.
2. **Change references without restarting.** Attach, replace, or remove a reference on a running server, and profile the same compatible sample evidence against different databases.

Further work on the roadmap, with scope and ordering still open:

- **Show leading profile hits in the terminal.** Extend the HUD beyond progress counts to preview results as they develop.
- **Expose richer profiling activity.** Let dashboards and library callers distinguish input receipt, in-progress profiling, and finished results.
- **Recover sessions after a restart.** Explore preserving enough evidence and analysis state to resume interrupted runs safely.
- **Manage memory for larger runs.** Measure reference and sample memory costs, then design useful budgets and admission controls.
- **Strengthen the library and diagnostics.** Review internal API boundaries and naming, improve failure messages, and extend panic-prone unwrap checks to tests.

These are planned directions rather than available features. Reference sketch generation may be explored alongside runtime reference management; its scope is still undecided.

## Sylph fork

**Axylph currently uses [our Sylph fork](https://github.com/nrminor/sylph).** Its exact revision is pinned in `Cargo.toml` and `Cargo.lock`.

The fork makes Sylph's sketch accumulation and profiling available as reusable library interfaces, so Axylph can accept new reads and profile them while keeping references loaded. It shares calculation code with the Sylph CLI; the fork provides the service integration that makes this workflow possible.
