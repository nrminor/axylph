# axylph

Axylph receives sequencing reads and accumulates a single-end sylph sketch for one sample. The library's `Session` periodically publishes cumulative metagenomic profiles; the command-line receiver remains sketch-only. **Neither retains raw reads or saves recovery state.**

The instrument client reads plain or gzip FASTQ and sends Arrow batches through Flight `DoPut`. The receiver runs on Axum and keeps one accumulator across uploads and disconnects. It returns upload-local counts after each batch is validated and sketched. Client logic is separate from terminal output so a future GUI can use the same interface.

```text
FASTQ file → client → Arrow Flight → receiver → cumulative sketch
                                        └──→ upload-local receipts
```

## Try the prototype

Install from the checkout with `cargo install --locked --path .` and ensure `~/.cargo/bin` is on your `PATH`. Then start the receiver from any directory:

```sh
axylph serve --sample sample-1
```

In another terminal, send a file:

```sh
axylph send reads.fastq.gz
```

The receiver listens at `127.0.0.1:50051` by default. Use `serve --listen HOST:PORT` and `send FILE --server http://HOST:PORT` to select another address. There is no authentication or TLS. Use a trusted local network; do not expose this prototype to the public internet.

Every upload to this receiver belongs to the same sample. `serve` requires `--sample`; `--c` defaults to 200, `--k` to 31, and exact single-end deduplication is enabled unless `--no-dedup` is supplied. Only positive c and k=21/31 are supported. These settings remain fixed until a new receiver is started. The CLI has no sample routing or explicit session-finalization operation yet.

Progress counts start at zero for each upload. A batch receipt follows validation and sketch accumulation; it does not promise a profile or durable storage. A completion report requires a clean input end, successful local parsing, a successful RPC, and matching client/server totals. Completion ends the upload, not the sample. There is no rollback or automatic retry.

On disconnect, accepted reads remain in memory. A submitted batch can finish after cancellation, and its receipt can be lost. **Do not replay an uncertain upload:** biological deduplication is not transport replay suppression. A new upload may briefly receive `RESOURCE_EXHAUSTED` while the previous batch finishes. Restarting the process loses all accumulated state.

## Input and limits

The client accepts four-line FASTQ records and detects gzip from the file contents. Concatenated gzip members are supported. It preserves the header, sequence letter case, and Phred+33 quality bytes. Sequence symbols must be IUPAC nucleotide letters; quality values must be printable bytes with one value per base. FASTA and wrapped FASTQ are not client inputs in this prototype.

Both commands accept `--batch-bytes` and `--batch-reads`. Defaults are 1 MiB of identifier/sequence/quality data and 1,024 reads per batch. The supported maxima are 1 MiB and 16,384 reads. One read must fit in a batch. Set the client limits no higher than the receiver limits. There is no total upload size limit.

The client has a one-batch channel and a parser buffer capped at 2 MiB with the default settings. The receiver decodes one batch at a time and permits one active upload. Flight messages are limited to 4 MiB. These are component limits, not a bound on total process memory: Arrow encoding, HTTP/2, and the runtime also use buffers. Marker counts and duplicate history grow with the sample; there is no total sketch-memory limit yet. Validation and sketching run on blocking threads, borrowing sequences from Arrow buffers without creating per-read copies.

The reusable client accepts a blocking `Read` source. Dropping `Client::send` signals cooperative cancellation to the request producer and parser, including a parser waiting for batch-channel capacity. It cannot interrupt an executing `Read::read`; use local files or a source with its own timeout. Once that read returns and cancellation is observed, its bytes are discarded and no subsequent source read is started. Dropping `send` does not wait for the blocking task to finish. Data already handed to the transport may still be accepted, and the receiver may observe request EOF rather than a cancellation status. Cancellation neither rolls back accepted reads nor finalizes the sample.

A peer claiming early completion cannot interrupt a blocked source: `send` may still await the parser to validate the input totals. Callers can impose a timeout by dropping the send future, subject to the same blocking-read limitation. The CLI handles Ctrl-C for receiver shutdown; it does not reconnect or resume an upload.

## Native interface

The experimental version 0 schema has three Arrow `Binary` columns: non-null `id`, non-null `sequence`, and nullable `quality`. A null quality means that quality scores are absent. Schema metadata contains `axylph.version = "0"`. One schema precedes the record batches. Dictionaries and IPC compression are rejected in this prototype.

`PutResult.app_metadata` contains JSON with upload-local cumulative `reads`, `bases`, and `complete` fields. Each validated and sketched batch produces a report with `complete: false`. At request EOF, the receiver sends the same counts with `complete: true`. Receiver validation errors and transport errors end the RPC with an error status. Other Flight methods return `UNIMPLEMENTED`.

Protocol validation checks the whole batch before sketch mutation. A malformed batch adds no reads, and later uploads may continue. A scientific counter-limit error can retain complete earlier reads from that batch without emitting a receipt for it; it stops further ingestion, but the accepted sketch remains inspectable. A panic while holding the sketch lock prevents reuse or inspection of potentially inconsistent state. There is no automatic reset.

A local parser error can look like request EOF to the receiver. The receiver can therefore send a completion receipt even though the source file is invalid. The client checks its parser result and input totals before it reports completion; it returns an error in this case. A receiver receipt alone does not prove that the source file was fully read. This transport prototype has no explicit session-finalization message.

This interface can change before 1.0. Direct HTTP FASTQ uploads, paired-end sketching, HTTP control operations, durable recovery, and a GUI are not implemented. Gzip support here is for client input; native network traffic is Arrow, not a FASTQ file.

## Observing the sketch

Library callers construct a `sylph::read_sketch::ReadSketcher` with explicit parameters and identity, then pass it to `Receiver::new(limits, sketcher)`. Keep a receiver clone before consuming another handle with `router()`:

```rust
let receiver = Receiver::new(limits, sketcher);
let app = receiver.clone().router();
let marker_count = receiver.inspect(|sketch| sketch.kmer_counts.len()).await?;
```

Clones share the accumulator and upload gate. `inspect` borrows a consistent processed prefix on a blocking thread; it neither copies the marker map nor resets duplicate history. It can run between batches of a live upload. Pending batches need not appear yet. Return only the owned information needed and keep callbacks short: ingestion waits for the callback's lock. Do not re-enter the receiver from a callback. Dropping the observation future does not stop a submitted callback. No sketch retrieval endpoint is exposed over the network.

## Observing cumulative profiles

A session owns the analysis; its receiver delivers reads to it. Supply a loaded `sylph::profile::Profiler` with fixed references, options, and an explicitly sized local worker pool, then start a fresh sample on your Tokio runtime (with its time driver enabled):

```rust
let session = Session::builder("sample-1", profiler)
    .c(50)
    .start()?;
let app = session.router(); // Flight uploads plus read-only GET /events.
let observations = session.observe();
// Serve app and consume observations on your runtime.
// When the operator explicitly finishes the sample:
let final_snapshot = session.finish().await?;
session.shutdown().await?;
```

The builder defaults to sylph's `SketchParams::default()` (c=200, k=31, exact deduplication), `Limits::default()`, and one-second refresh eligibility. `.sketch_params(...)` replaces the complete scientific configuration; `.c(...)`, `.k(...)`, and `.deduplication(...)` replace individual fields. Later calls win. `.limits(...)` and `.refresh_interval(...)` override the operational settings. Fully explicit `Session::start(limits, sample_name, sketch_params, profiler, refresh_interval)` uses the same initialization path. Startup rejects incompatible sketch/reference parameters before admitting reads; default c=200 therefore needs overriding for c=50/100 references. It never silently changes scientific settings.

`observe()` returns an owned stream: current state first, then coalesced changes. Pin it when using `StreamExt::next`. Observations expose current processed read/base totals, lifecycle and failure information, and an optional shared snapshot. A snapshot carries its own checkpoint and read-only sylph `Profile`; current progress can be ahead of its coverage. Duplicates and reads contributing no markers still count. Holding an older observation retains its result unchanged, but slow consumers can skip intermediate states. This is not a retained history; `Session::router()` exposes the HTTP adapter described below.

One worker captures a copy of cumulative marker counts and metadata with its checkpoint, then profiles outside the ingestion lock using resident references. It does not copy raw reads, reference genomes, or duplicate history, nor pre-copy pending inputs. An ordinary refresh error preserves the previous snapshot and reports the attempted checkpoint; another automatic attempt requires newer evidence. Before the first success, no profile is available. Refresh eligibility is not a calculation deadline, and large-reference memory use, copy pauses, and sustained throughput have not been characterized.

`finish()` closes read admission and drains admitted work, then returns a shared profile covering exactly the terminal processed checkpoint. An existing successful snapshot with that coverage is reused; otherwise one required terminal calculation runs. `Observation::finish_outcome()` retains the success or failure after shutdown. Finality belongs to this recorded outcome, not a mutable flag on previously published snapshots. A failed finish keeps admission closed and older results non-final; repeated calls observe the same outcome without retrying. Ingestion errors remain independently visible and a final profile does not prove a successful upload.

`shutdown()` closes read admission, stops periodic scheduling, drains admitted blocking work, and awaits active profiling without waiting for upload EOF. Repeated or cancelled waits do not restart or cancel that work. Dropping the owner requests stopping but cannot await synchronous work. The first terminal request wins: shutdown honors an already accepted finish, but finish after shutdown cannot restart the session. Shutdown alone does not finalize the sample or guarantee a profile covering the last read. Library consumers own their runtime and choose the profiler's worker count; neither count is a whole-process CPU quota.

## HTTP observations (library)

`Session::router()` serves Flight over HTTP/2 and read-only `GET /events` over HTTP/1.1 or HTTP/2 on the same listener. `Receiver::router()` remains Flight-only. The CLI still constructs a sketch-only receiver; it does not yet expose profiles or SSE. The application must retain its `Session` owner while serving: retaining a router does not keep the worker owner alive, and an SSE disconnect does not stop profiling.

`/events` returns `text/event-stream` with JSON data, keepalive comments, and `Cache-Control: no-store`. Each connection starts with a `snapshot` event containing `{ "status": ..., "profile": ... }`. Later `snapshot` events replace the entire client view when the profile changes; `status` events replace only its metadata. All fields in a message come from one immutable observation. Intermediate observations may be skipped, but a new profile is included before metadata can refer to it. Reconnection starts from current state without replay, event IDs, or a separate initial GET. A `profile: null` clears any previously displayed result; a successful empty profile instead has empty `rows`.

Status contains `sample_name`, `state` (`open`, `stopping`, or `stopped`), `processed`, `profile_checkpoint`, `ingestion_error`, `refresh_error`, and `finish`. Errors are null or objects with a machine-readable `code` and diagnostic `message`; refresh errors also identify the attempted `checkpoint`. Finish is null until an outcome exists, then `{ "status": "succeeded", "checkpoint": ... }` or `{ "status": "failed", "error": ... }`. Stopped state and network EOF do not imply successful finish. Browser clients should close their `EventSource` after receiving stopped state, otherwise normal EventSource reconnection can repeat the terminal view.

Profiles include their checkpoint, sample name, scientific options, and complete replacement rows. Checkpoint read/base totals and observed marker counts are decimal strings, preserving integer precision in browser JSON. Estimates are unrounded numbers: ANI fields end in `_fraction`, taxonomic abundance in `_percent`, and `sequence_estimate` has `kind` (`percent` or `estimated_read_count`) and `value`. `adjustment` distinguishes `lambda` with a value from `low` and `high`. Intervals are two-element arrays or null; the options retain whether intervals were requested. Adjusted ANI is not capped at one on the wire. Consumers choose display rounding and clipping; errors must not be inferred from empty rows or null intervals.

Routine progress-only events are spaced at least 250 ms after the preceding event on each subscription, coalescing pending progress to the latest state. Initial snapshots, new profiles, and error/lifecycle/finish changes bypass that wait, carry current progress, and restart the progress interval. This limits server stream emissions, not network arrival times; backpressure and proxy buffering can delay or bunch deliveries. No queue of old observations is retained. Profiling still uses its existing eligibility timer; strict minimum profiling-start spacing is not yet implemented.

Reconnection, slow-client behavior, HTTP finality, and error paths need subsequent focused tests. Profile serialization borrows the scientific rows, but each connection owns its serialized payload; large-payload CPU/memory costs and deployment proxy buffering have not been measured. Authentication, TLS, and cross-origin policy remain the host application's responsibility.

## Development

Use Mise 2026.10.22 or newer. Rust is pinned in `mise.toml` and `rust-toolchain.toml`; keep these versions and the Cargo Rust requirement in sync. Axylph selects sylph's `profile-core` without its CLI or FASTX/file-I/O features, so sylph does not add native compression libraries or a CMake requirement to this build.

```sh
mise trust
mise install
mise run check
mise run ci-validate
```

`check` runs formatting, Clippy, nextest, documentation checks, and doctests when `src/lib.rs` exists. Tests exercise the client/server interface and public Flight input handling. Accumulation, profile-publication, and finish tests use checked-in expectations generated from unchanged upstream revision `1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9`; they do not run the adapted code to generate their oracle. The JSON fixtures record provenance, checksums, and generation environment. The profiling tracers cover 23 synthetic reads and their cumulative profile at CLI precision, including library finality retained after shutdown and a real HTTP/1.1 SSE subscription alongside HTTP/2 Flight on one listener. The SSE tracer checks initial absence and independently expected profile values; it does not establish reconnect or delivery-rate behavior. These checks do not establish unrounded equivalence, overlapping computation/ingestion, lifecycle-race behavior, or large-workload performance. Build and verification tasks use `Cargo.lock` without updating dependencies.

Controlled-clock internal tests exercise progress coalescing and a lifecycle change bypassing pending progress on the real SSE response body with a controlled observation source. They establish server emission eligibility at the 250 ms boundary, not TCP arrival spacing or scientific scheduling. The end-to-end tracer remains the independent transport/scientific check.

`.gitignore` denies files by default. Add each source, test, or configuration file to its allowlist when it should be tracked. Keep imports from the same root in one `use` statement.

Library errors use `axylph::Error`, defined with `thiserror` in `src/error.rs`; session finish and shutdown share errors through `Arc<Error>` so repeated callers can observe the same failure. Application failures have named variants; dependency failures wrap their source errors. The CLI installs `color-eyre` for reporting and adds file or address context. Library callers do not need to install a reporting handler.

Dependencies serve specific roles: Arrow and Flight handle structured reads and IPC, Tonic and Axum serve HTTP/2, and Tokio plus stream adapters provide bounded asynchronous transfer. `seq_io` bounds FASTQ parsing; `flate2` handles gzip, including concatenated members, with its Rust backend. Serde and JSON encode the progress wire format. Clap handles CLI parsing and help. Sylph owns the scientific algorithm, sketch types, and public profile representation; only its embeddable profiling capability is enabled, not CLI, file readers, or native compression. Replacing these libraries means taking on their behavior and maintenance, not just removing package names. Optional tracing-span capture in `color-eyre` is disabled because the application does not set up tracing spans.

Use `mise run bc`, `mise run bb`, or `mise run bn` to watch checks, builds, or tests with Bacon. Mise enables mr-boxington for Rust builds. Use `mise run mt` to inspect its activity. These settings do not by themselves prove cache hits.

## Upstream reference

```sh
mise bootstrap repos apply
```

This clones unchanged upstream sylph into `.agents/repos/sylph` for source reference. Builds fetch the maintained fork at `https://github.com/nrminor/sylph`, pinned to published revision `1c9ec8658c5566a424a97fda2512b2a332dabf2c` with default features disabled and `profile-core` enabled. `Cargo.lock` records that Git source; no local Cargo patch or reference checkout is required to build. Planning notes and research checkouts remain local.

The fork preserves upstream CPU dispatch. At this revision AVX2 extraction omits up to three trailing windows that scalar extraction includes. The scientific fixtures avoid sampled omitted windows; passing them is not a general scalar/AVX2 equivalence claim. Axylph has not changed this extraction behavior. Exact deduplication also retains sylph's signature-eligibility and count-four rules; it does not suppress every repeated read forever.
