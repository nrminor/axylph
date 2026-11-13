//! CLI and partial configuration resolution for the executable.

use std::{env, fs, net::SocketAddr, path::PathBuf, str::FromStr, time::SystemTime};

use axylph::Limits;
use clap::{Args, Parser, Subcommand};
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::Deserialize;

#[derive(Parser)]
#[command(
    version,
    about = "Accumulate sequencing read sketches, with optional reference profiling."
)]
struct Cli {
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
    /// Print the configuration file's JSON Schema (Draft 7).
    Schema,
}

// Defaults make fields optional; the schema overrides exclude null, which TOML cannot express.
#[derive(Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FileConfig {
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

pub enum Command {
    Serve {
        listen: SocketAddr,
        sample: String,
        c: usize,
        k: usize,
        no_dedup: bool,
        reference: Option<PathBuf>,
        profile_workers: usize,
        refresh_interval_ms: u64,
    },
    Send {
        path: PathBuf,
        server: String,
    },
    Schema,
}

pub struct Resolved {
    pub command: Command,
    pub limits: Limits,
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

fn load(path: Option<PathBuf>) -> Result<(FileConfig, PathBuf)> {
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
    let config = match fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text)
            .wrap_err_with(|| format!("invalid configuration {}", path.display())),
        Err(error) if implicit && error.kind() == std::io::ErrorKind::NotFound => {
            Ok(FileConfig::default())
        }
        Err(error) => {
            Err(error).wrap_err_with(|| format!("could not read configuration {}", path.display()))
        }
    }?;
    Ok((config, path))
}

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
) -> Result<(Option<PathBuf>, usize, u64)> {
    let reference = if let Some(path) = args.reference.clone() {
        Some(std::path::absolute(path)?)
    } else if let Some(path) = environment::<PathBuf>("AXYLPH_REFERENCE")? {
        Some(std::path::absolute(path)?)
    } else {
        serve.reference.clone().map(|path| {
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
    let profile_workers = workers.unwrap_or_else(worker_count);
    let refresh_interval_ms = interval.unwrap_or(1000);
    if profile_workers == 0 || refresh_interval_ms == 0 {
        bail!("profile workers and refresh interval must be positive");
    }
    Ok((reference, profile_workers, refresh_interval_ms))
}

pub fn parse() -> Result<Resolved> {
    let cli = Cli::parse();
    if matches!(cli.command, Some(CliCommand::Schema)) {
        return Ok(Resolved {
            command: Command::Schema,
            limits: Limits::default(),
        });
    }
    let (file, config_path) = load(cli.config)?;
    let limits = Limits::new(
        resolve(
            cli.batch_bytes,
            "AXYLPH_BATCH_BYTES",
            file.batch_bytes,
            1_048_576,
        )?,
        resolve(
            cli.batch_reads,
            "AXYLPH_BATCH_READS",
            file.batch_reads,
            1024,
        )?,
    )?;
    let command = match cli.command {
        Some(CliCommand::Send { path, server }) => {
            if cli.serve.sample.is_some()
                || cli.serve.listen.is_some()
                || cli.serve.c.is_some()
                || cli.serve.k.is_some()
                || cli.serve.dedup
                || cli.serve.no_dedup
                || cli.serve.reference.is_some()
                || cli.serve.profile_workers.is_some()
                || cli.serve.refresh_interval_ms.is_some()
            {
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
        Some(CliCommand::Serve) | None => {
            let serve = file.serve.unwrap_or_default();
            let (reference, profile_workers, refresh_interval_ms) =
                profiling(&cli.serve, &serve, &config_path)?;
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
            Command::Serve {
                listen,
                sample,
                c: resolve(cli.serve.c, "AXYLPH_C", serve.c, 200)?,
                k: resolve(cli.serve.k, "AXYLPH_K", serve.k, 31)?,
                no_dedup: !resolve(dedup_arg, "AXYLPH_DEDUP", serve.dedup, true)?,
                reference,
                profile_workers,
                refresh_interval_ms,
            }
        }
    };
    Ok(Resolved { command, limits })
}

pub fn print_schema() -> Result<()> {
    let schema = SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<FileConfig>();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}
