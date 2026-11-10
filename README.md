# axylph

Axylph receives sequencing reads and accumulates a single-end sylph sketch for one sample. **It does not retain reads, produce metagenomic profiles, or save recovery state.**

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

Every upload to this receiver belongs to the same sample. `serve` requires `--sample`; `--c` defaults to 200, `--k` to 31, and exact single-end deduplication is enabled unless `--no-dedup` is supplied. Only positive c and k=21/31 are supported. These settings remain fixed until a new receiver is started. There is no sample routing or explicit session-finalization operation yet.

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

This interface can change before 1.0. Direct HTTP FASTQ uploads, paired-end sketching, profile snapshots, durable recovery, and a GUI are not implemented. Gzip support here is for client input; native network traffic is Arrow, not a FASTQ file.

## Observing the sketch

Library callers construct a `sylph::read_sketch::ReadSketcher` with explicit parameters and identity, then pass it to `Receiver::new(limits, sketcher)`. Keep a receiver clone before consuming another handle with `router()`:

```rust
let receiver = Receiver::new(limits, sketcher);
let app = receiver.clone().router();
let marker_count = receiver.inspect(|sketch| sketch.kmer_counts.len()).await?;
```

Clones share the accumulator and upload gate. `inspect` borrows a consistent processed prefix on a blocking thread; it neither copies the marker map nor resets duplicate history. It can run between batches of a live upload. Pending batches need not appear yet. Return only the owned information needed and keep callbacks short: ingestion waits for the callback's lock. Do not re-enter the receiver from a callback. Dropping the observation future does not stop a submitted callback. No sketch retrieval endpoint is exposed over the network.

## Development

Use Mise 2026.10.22 or newer. Rust is pinned in `mise.toml` and `rust-toolchain.toml`; keep these versions and the Cargo Rust requirement in sync. Axylph selects sylph's scientific core without its CLI, file-I/O, or profiling features, so sylph does not add native compression libraries or a CMake requirement to this build.

```sh
mise trust
mise install
mise run check
mise run ci-validate
```

`check` runs formatting, Clippy, nextest, documentation checks, and doctests when `src/lib.rs` exists. Tests exercise the client/server interface and public Flight input handling. Accumulation tests use checked-in expectations generated by unchanged upstream revision `1f7fcb53aa20b592d8b3b97f75d9971a60eb06e9`; they do not run the adapted accumulator to generate their oracle. The JSON fixture records its source, checksums, and generation environment. Build and verification tasks use `Cargo.lock` without updating dependencies.

`.gitignore` denies files by default. Add each source, test, or configuration file to its allowlist when it should be tracked. Keep imports from the same root in one `use` statement.

The library returns `axylph::Error`, defined with `thiserror` in `src/error.rs`. Application failures have named variants; dependency failures wrap their source errors. The CLI installs `color-eyre` for reporting and adds file or address context. Library callers do not need to install a reporting handler.

Dependencies serve specific roles: Arrow and Flight handle structured reads and IPC, Tonic and Axum serve HTTP/2, and Tokio plus stream adapters provide bounded asynchronous transfer. `seq_io` bounds FASTQ parsing; `flate2` handles gzip, including concatenated members, with its Rust backend. Serde and JSON encode the progress wire format. Clap handles CLI parsing and help. Sylph owns the scientific algorithm and shared sketch types; its default features are disabled so its CLI, file readers, profiling, and native compression dependencies are not compiled. Replacing these libraries means taking on their behavior and maintenance, not just removing package names. Optional tracing-span capture in `color-eyre` is disabled because the application does not set up tracing spans.

Use `mise run bc`, `mise run bb`, or `mise run bn` to watch checks, builds, or tests with Bacon. Mise enables mr-boxington for Rust builds. Use `mise run mt` to inspect its activity. These settings do not by themselves prove cache hits.

## Upstream reference

```sh
mise bootstrap repos apply
```

This clones unchanged upstream sylph into `.agents/repos/sylph` for source reference. Production builds instead fetch `https://github.com/nrminor/sylph` at exact revision `b72776929d93caae9e576f1342078da6d57ab929` through Cargo with `default-features = false`; they never depend on an ignored local checkout. Planning notes and reference checkouts remain local.

The fork preserves upstream CPU dispatch. At this revision AVX2 extraction omits up to three trailing windows that scalar extraction includes. The scientific fixtures avoid sampled omitted windows; passing them is not a general scalar/AVX2 equivalence claim. Axylph has not changed this extraction behavior. Exact deduplication also retains sylph's signature-eligibility and count-four rules; it does not suppress every repeated read forever.
