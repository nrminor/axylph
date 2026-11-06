# axylph

Axylph will receive sequencing reads and use sylph to produce metagenomic profiles. The current prototype transfers and validates reads. **It does not run sylph, retain reads, or produce profiles.**

The instrument client reads plain or gzip FASTQ and sends Arrow batches through Flight `DoPut`. The receiver runs on Axum. It returns counts after each valid batch. Client logic is separate from terminal output so a future GUI can use the same interface.

```text
FASTQ file → client → Arrow Flight → receiver → validation counts
```

## Try the prototype

Install from the checkout with `cargo install --locked --path .` and ensure `~/.cargo/bin` is on your `PATH`. Then start the receiver from any directory:

```sh
axylph serve
```

In another terminal, send a file:

```sh
axylph send reads.fastq.gz
```

The receiver listens at `127.0.0.1:50051` by default. Use `serve --listen HOST:PORT` and `send FILE --server http://HOST:PORT` to select another address. There is no authentication or TLS. Use a trusted local network; do not expose this prototype to the public internet.

Progress means **validated by the receiver**, not sketched, profiled, or saved. Counts start at zero for each upload. A completion report requires a clean input end, successful local parsing, a successful RPC, and matching client/server totals. On failure, earlier counts remain observations of a partial upload. There is no rollback or automatic retry.

## Input and limits

The client accepts four-line FASTQ records and detects gzip from the file contents. Concatenated gzip members are supported. It preserves the header, sequence letter case, and Phred+33 quality bytes. Sequence symbols must be IUPAC nucleotide letters; quality values must be printable bytes with one value per base. FASTA and wrapped FASTQ are not client inputs in this prototype.

Both commands accept `--batch-bytes` and `--batch-reads`. Defaults are 1 MiB of identifier/sequence/quality data and 1,024 reads per batch. The supported maxima are 1 MiB and 16,384 reads. One read must fit in a batch. Set the client limits no higher than the receiver limits. There is no total upload size limit.

The client has a one-batch channel and a parser buffer capped at 2 MiB with the default settings. The receiver decodes one batch at a time and permits one active upload. Flight messages are limited to 4 MiB. These are component limits, not a bound on total process memory: Arrow encoding, HTTP/2, and the runtime also use buffers.

The reusable client accepts a blocking `Read` source. Dropping a transfer stops parsing at the next record or channel check. It cannot interrupt a source blocked inside `read`; use local files or a source with its own timeout. The CLI handles Ctrl-C for receiver shutdown. It does not reconnect or resume an upload.

## Native interface

The experimental version 0 schema has three Arrow `Binary` columns: non-null `id`, non-null `sequence`, and nullable `quality`. A null quality means that quality scores are absent. Schema metadata contains `axylph.version = "0"`. One schema precedes the record batches. Dictionaries and IPC compression are rejected in this prototype.

`PutResult.app_metadata` contains JSON with cumulative `reads`, `bases`, and `complete` fields. Each validated batch produces a report with `complete: false`. At request EOF, the receiver sends the same counts with `complete: true`. Receiver validation errors and transport errors end the RPC with an error status. Other Flight methods return `UNIMPLEMENTED`.

A local parser error can look like request EOF to the receiver. The receiver can therefore send a completion receipt even though the source file is invalid. The client checks its parser result and input totals before it reports completion; it returns an error in this case. A receiver receipt alone does not prove that the source file was fully read. This transport prototype has no explicit session-finalization message.

This interface can change before 1.0. Direct HTTP FASTQ uploads, scientific sketch accumulation, profile snapshots, durable recovery, and a GUI are not implemented. Gzip support here is for client input; native network traffic is Arrow, not a FASTQ file.

## Development

Use Mise 2026.10.22 or newer. Rust is pinned in `mise.toml` and `rust-toolchain.toml`; keep these versions and the Cargo Rust requirement in sync.

```sh
mise trust
mise install
mise run check
mise run ci-validate
```

`check` runs formatting, Clippy, nextest, documentation checks, and doctests when `src/lib.rs` exists. Tests exercise the client/server interface and public Flight input handling. Build and verification tasks use `Cargo.lock` without updating dependencies.

Use `mise run bc`, `mise run bb`, or `mise run bn` to watch checks, builds, or tests with Bacon. Mise enables mr-boxington for Rust builds. Use `mise run mt` to inspect its activity. These settings do not by themselves prove cache hits.

## Upstream reference

```sh
mise bootstrap repos apply
```

This clones sylph into `.agents/repos/sylph` for source reference. It is not an axylph dependency. Planning notes and reference checkouts remain local and are ignored by version control.
