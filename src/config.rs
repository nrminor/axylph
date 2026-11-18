//! CLI and partial configuration resolution for the executable.

use std::{env, fs, net::SocketAddr, path::PathBuf, str::FromStr, time::SystemTime};

use axylph::Limits;
use clap::{Args, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::Deserialize;
use sylph::read_sketch::{Deduplication, SketchParams};

use crate::server;

#[derive(Parser)]
#[command(
    version,
    about = "Accumulate sequencing read sketches, with optional reference profiling."
)]
struct Cli {
    /// Terminal presentation (auto or plain).
    #[arg(long, global = true, value_enum)]
    display: Option<DisplayMode>,
    /// Path to a TOML configuration file.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Maximum identifier, sequence, and quality bytes per decoded batch.
    #[arg(long, global = true)]
    batch_bytes: Option<usize>,
    /// Maximum reads per batch.
    #[arg(long, global = true)]
    batch_reads: Option<usize>,
    #[command(flatten)]
    serve: ServeArgs,
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Args, Default)]
struct ServeArgs {
    /// Reference database for profiling.
    #[arg(long, global = true)]
    reference: Option<PathBuf>,
    /// Number of profiling workers (positive).
    #[arg(long, global = true)]
    profile_workers: Option<usize>,
    /// Minimum refresh interval in milliseconds (positive).
    #[arg(long, global = true)]
    refresh_interval_ms: Option<u64>,
    /// Address for the Flight service.
    #[arg(long, global = true)]
    listen: Option<SocketAddr>,
    /// Sample shared by every upload to this receiver.
    #[arg(long, global = true)]
    sample: Option<String>,
    /// Sylph sampling factor (positive).
    #[arg(long, global = true)]
    c: Option<usize>,
    /// K-mer length (21 or 31).
    #[arg(long, global = true)]
    k: Option<usize>,
    /// Enable exact single-end deduplication.
    #[arg(long, global = true, conflicts_with = "no_dedup")]
    dedup: bool,
    /// Count every sampled occurrence instead of exact single-end deduplication.
    #[arg(long, global = true)]
    no_dedup: bool,
}

impl ServeArgs {
    const fn supplied(&self) -> bool {
        self.sample.is_some()
            || self.listen.is_some()
            || self.c.is_some()
            || self.k.is_some()
            || self.dedup
            || self.no_dedup
            || self.reference.is_some()
            || self.profile_workers.is_some()
            || self.refresh_interval_ms.is_some()
    }
}

#[derive(Subcommand)]
enum CliCommand {
    /// Accumulate one sample on a trusted network. No authentication or TLS.
    Serve,
    /// Send a plain or gzip FASTQ file and report validation progress.
    Send {
        /// Path to a four-line-per-read FASTQ file, optionally gzip compressed.
        path: PathBuf,
        /// HTTP address of the Flight service.
        #[arg(long)]
        server: Option<String>,
    },
    /// Finish ingestion and print the final profile as JSON.
    Finish {
        /// HTTP origin of the service.
        #[arg(long)]
        server: Option<String>,
    },
    /// Print the configuration file's JSON Schema (Draft 7).
    Schema,
}

// Defaults make fields optional; the schema overrides exclude null, which TOML cannot express.
#[derive(Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "DisplayMode")]
    display: Option<DisplayMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "usize")]
    batch_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "usize")]
    batch_reads: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "FileServe")]
    serve: Option<FileServe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "FileSend")]
    send: Option<FileSend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "FileSend")]
    finish: Option<FileSend>,
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FileServe {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    reference: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "usize")]
    profile_workers: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "u64")]
    refresh_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    sample: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    listen: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "usize")]
    c: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "usize")]
    k: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool")]
    dedup: Option<bool>,
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FileSend {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    server: Option<String>,
}

/// Resolved executable operation.
pub enum Command {
    /// Run the receiver with the resolved settings.
    Serve(server::Settings),
    /// Upload a FASTQ file.
    Send {
        /// Input FASTQ path.
        path: PathBuf,
        /// Receiver HTTP origin.
        server: String,
    },
    /// Finish the receiver's current sample.
    Finish {
        /// Receiver HTTP origin.
        server: String,
    },
    /// Print the configuration schema.
    Schema,
}

/// Configuration resolved for the selected operation.
pub struct Resolved {
    /// Operation to execute.
    pub command: Command,
    /// Limits for ingestion or upload batches.
    pub limits: Limits,
    /// Terminal presentation mode.
    pub display: DisplayMode,
}

/// Terminal presentation preference.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DisplayMode {
    #[default]
    /// Select interactive presentation when available.
    Auto,
    /// Use plain output.
    Plain,
}

impl FromStr for DisplayMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(value, false)
    }
}

fn environment<T: FromStr>(name: &str) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|error| eyre!("invalid {name}: {error}")),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).wrap_err_with(|| format!("invalid {name}")),
    }
}

fn generated_sample() -> Result<String> {
    let nanoseconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .wrap_err("cannot generate sample name: system clock is before the Unix epoch")?
        .as_nanos();
    Ok(format!("sample-{nanoseconds:x}-{:x}", std::process::id()))
}

// Evaluate lower-priority sources only when needed, so overridden invalid environment
// values do not prevent a valid explicit argument from taking effect.
fn resolve<T: FromStr>(arg: Option<T>, name: &str, file: Option<T>, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    if let Some(value) = arg {
        return Ok(value);
    }
    Ok(environment(name)?.or(file).unwrap_or(default))
}

fn load(path: Option<PathBuf>) -> Result<(FileConfig, PathBuf, Option<PathBuf>)> {
    let (path, implicit) = path.map_or_else(
        || {
            env::var_os("AXYLPH_CONFIG").map_or_else(
                || (PathBuf::from("axylph.toml"), true),
                |path| (PathBuf::from(path), false),
            )
        },
        |path| (path, false),
    );
    let path = std::path::absolute(path)?;
    let (config, loaded) = match fs::read_to_string(&path) {
        Ok(text) => (
            toml::from_str(&text)
                .wrap_err_with(|| format!("invalid configuration {}", path.display()))?,
            Some(path.clone()),
        ),
        Err(error) if implicit && error.kind() == std::io::ErrorKind::NotFound => {
            (FileConfig::default(), None)
        }
        Err(error) => {
            return Err(error)
                .wrap_err_with(|| format!("could not read configuration {}", path.display()));
        }
    };
    Ok((config, path, loaded))
}

/// Choose a conservative default worker count for the host.
pub fn worker_count() -> usize {
    if std::thread::available_parallelism().is_ok_and(|count| count.get() >= 2) {
        2
    } else {
        1
    }
}

fn profiling(
    args: &ServeArgs,
    serve: &FileServe,
    config_path: &std::path::Path,
) -> Result<Option<server::Profiling>> {
    let reference = if let Some(path) = &args.reference {
        Some(std::path::absolute(path)?)
    } else if let Some(path) = environment::<PathBuf>("AXYLPH_REFERENCE")? {
        Some(std::path::absolute(path)?)
    } else {
        serve.reference.as_ref().map(|path| {
            config_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("/"))
                .join(path)
        })
    };
    let workers = if let Some(value) = args.profile_workers {
        Some(value)
    } else {
        environment("AXYLPH_PROFILE_WORKERS")?.or(serve.profile_workers)
    };
    let interval = if let Some(value) = args.refresh_interval_ms {
        Some(value)
    } else {
        environment("AXYLPH_REFRESH_INTERVAL_MS")?.or(serve.refresh_interval_ms)
    };
    if reference.is_none() && (workers.is_some() || interval.is_some()) {
        bail!("profiling options require a reference");
    }
    let Some(reference) = reference else {
        return Ok(None);
    };
    let workers = workers.unwrap_or_else(worker_count);
    let interval = std::time::Duration::from_millis(interval.unwrap_or(1000));
    Ok(Some(server::Profiling::new(reference, workers, interval)?))
}

/// Parse arguments and resolve active configuration sources.
///
/// # Errors
/// Returns an error if an active configuration source is invalid or cannot be read.
pub fn parse() -> Result<Resolved> {
    let cli = Cli::parse();
    if matches!(cli.command, Some(CliCommand::Schema)) {
        return Ok(Resolved {
            command: Command::Schema,
            limits: Limits::default(),
            display: DisplayMode::Auto,
        });
    }
    resolve_cli(cli)
}

fn resolve_cli(cli: Cli) -> Result<Resolved> {
    let (file, config_path, loaded_path) = load(cli.config.clone())?;
    let display = resolve(
        cli.display,
        "AXYLPH_DISPLAY",
        file.display,
        DisplayMode::Auto,
    )?;
    if let Some(CliCommand::Finish { server }) = cli.command {
        if cli.batch_bytes.is_some() || cli.batch_reads.is_some() || cli.serve.supplied() {
            bail!("serve and batch options cannot be used with finish");
        }
        return Ok(Resolved {
            command: Command::Finish {
                server: resolve(
                    server,
                    "AXYLPH_SERVER",
                    file.finish.unwrap_or_default().server,
                    "http://127.0.0.1:50051".to_owned(),
                )?,
            },
            limits: Limits::default(),
            display,
        });
    }
    let limits = batch_limits(&cli, &file)?;
    let command = match cli.command {
        Some(CliCommand::Send { path, server }) => {
            if cli.serve.supplied() {
                bail!("serve options cannot be used with send");
            }
            Command::Send {
                path,
                server: resolve(
                    server,
                    "AXYLPH_SERVER",
                    file.send.unwrap_or_default().server,
                    "http://127.0.0.1:50051".to_owned(),
                )?,
            }
        }
        Some(CliCommand::Schema) => Command::Schema,
        Some(CliCommand::Finish { .. }) => bail!("finish was already resolved"),
        Some(CliCommand::Serve) | None => {
            let serve = file.serve.unwrap_or_default();
            let profiling = profiling(&cli.serve, &serve, &config_path)?;
            let optional_sample = match cli.serve.sample {
                Some(sample) => Some(sample),
                None => environment("AXYLPH_SAMPLE")?.or(serve.sample),
            };
            let sample = match optional_sample {
                Some(sample) => sample,
                None => generated_sample()?,
            };
            let listen = if let Some(listen) = cli.serve.listen {
                listen
            } else if let Some(listen) = environment("AXYLPH_LISTEN")? {
                listen
            } else {
                serve
                    .listen
                    .unwrap_or_else(|| "127.0.0.1:50051".to_owned())
                    .parse()
                    .wrap_err("invalid serve listen address")?
            };
            let dedup_arg = if cli.serve.dedup {
                Some(true)
            } else if cli.serve.no_dedup {
                Some(false)
            } else {
                None
            };
            let c = resolve(cli.serve.c, "AXYLPH_C", serve.c, 200)?;
            let k = resolve(cli.serve.k, "AXYLPH_K", serve.k, 31)?;
            let dedup = resolve(dedup_arg, "AXYLPH_DEDUP", serve.dedup, true)?;
            let deduplication = if dedup {
                Deduplication::Exact
            } else {
                Deduplication::Disabled
            };
            let params = SketchParams {
                c,
                k,
                deduplication,
            };
            let settings = server::Settings {
                listen,
                sample,
                params,
                limits,
                profiling,
                config_path: loaded_path,
            };
            Command::Serve(settings)
        }
    };
    Ok(Resolved {
        command,
        limits,
        display,
    })
}

fn batch_limits(cli: &Cli, file: &FileConfig) -> Result<Limits> {
    let bytes = resolve(
        cli.batch_bytes,
        "AXYLPH_BATCH_BYTES",
        file.batch_bytes,
        1_048_576,
    )?;
    let reads = resolve(
        cli.batch_reads,
        "AXYLPH_BATCH_READS",
        file.batch_reads,
        1024,
    )?;
    Ok(Limits::new(bytes, reads)?)
}

/// Print the partial configuration JSON Schema.
///
/// # Errors
/// Returns an error if the schema cannot be serialized.
pub fn print_schema() -> Result<()> {
    let schema = SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<FileConfig>();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}
