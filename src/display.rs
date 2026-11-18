//! Stderr-only presentation. No raw mode or alternate screen is used.
#[cfg(feature = "hud")]
use std::fmt::Write;
use std::{
    io::IsTerminal,
    time::{Duration, Instant},
};

use crate::config::DisplayMode;
use axylph::{Observation, Progress, SessionState};

#[cfg(feature = "hud")]
use color_eyre::owo_colors::OwoColorize;
#[cfg(feature = "hud")]
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Renders progress and diagnostics to stderr.
pub struct Display {
    #[cfg(feature = "hud")]
    bar: Option<ProgressBar>,
    lines: Vec<String>,
    pending_progress: Option<Progress>,
    last: Instant,
}

impl Display {
    /// Creates a display for the requested mode.
    pub(crate) fn new(mode: DisplayMode) -> Self {
        #[cfg(feature = "hud")]
        let bar = if matches!(mode, DisplayMode::Auto)
            && std::io::stderr().is_terminal()
            && std::env::var_os("TERM").is_none_or(|term| term != "dumb")
        {
            ProgressStyle::with_template("{msg}")
                .ok()
                .and_then(|style| {
                    let bar =
                        ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr_with_hz(4));
                    bar.set_style(style);
                    (!bar.is_hidden()).then_some(bar)
                })
        } else {
            None
        };
        #[cfg(not(feature = "hud"))]
        let _ = (mode, std::io::stderr().is_terminal());
        Self {
            #[cfg(feature = "hud")]
            bar,
            lines: Vec::new(),
            pending_progress: None,
            last: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        }
    }

    /// Returns whether the interactive display is active.
    pub(crate) const fn active(&self) -> bool {
        #[cfg(feature = "hud")]
        {
            self.bar.is_some()
        }
        #[cfg(not(feature = "hud"))]
        {
            let _ = self;
            false
        }
    }

    /// Prints a sanitized diagnostic.
    pub(crate) fn message(&self, message: &str) {
        let _ = self;
        let message = sanitize(message);
        #[cfg(feature = "hud")]
        if let Some(bar) = &self.bar
            && !bar.is_hidden()
        {
            bar.println(&message);
            return;
        }
        eprintln!("{message}");
    }

    /// Runs output while the interactive display is suspended.
    pub(crate) fn suspend_output<R>(&self, output: impl FnOnce() -> R) -> R {
        let _ = self;
        #[cfg(feature = "hud")]
        if let Some(bar) = &self.bar {
            return bar.suspend(output);
        }
        output()
    }

    /// Retains display lines and redraws when eligible or forced.
    pub(crate) fn update(&mut self, lines: Vec<String>, force: bool) {
        self.pending_progress = None;
        self.lines = lines;
        if !self.active() || (!force && self.last.elapsed() < Duration::from_millis(250)) {
            return;
        }
        self.last = Instant::now();
        #[cfg(feature = "hud")]
        if let Some(bar) = &self.bar {
            bar.set_message(self.lines.join("\n"));
            if force {
                bar.suspend(|| {});
            }
        }
    }

    /// Reports cumulative upload progress.
    pub(crate) fn progress(&mut self, progress: Progress) {
        if self.active() {
            self.upload_progress(
                progress,
                progress.complete || self.last.elapsed() >= Duration::from_millis(250),
            );
        } else if progress.complete || self.last.elapsed() >= Duration::from_millis(250) {
            self.last = Instant::now();
            self.message(&format!(
                "validated: {} reads, {} bases{}",
                progress.reads,
                progress.bases,
                if progress.complete {
                    " (upload complete)"
                } else {
                    ""
                }
            ));
        }
    }

    fn upload_progress(&mut self, progress: Progress, eligible: bool) {
        self.pending_progress = Some(progress);
        if eligible {
            self.update(progress_lines(progress), progress.complete);
        }
    }

    #[cfg(feature = "hud")]
    fn final_message(&self) -> String {
        self.pending_progress.map_or_else(
            || self.lines.join("\n"),
            |progress| progress_lines(progress).join("\n"),
        )
    }
}

fn progress_lines(progress: Progress) -> Vec<String> {
    vec![
        format!("{} reads valid", count(progress.reads)),
        format!("{} bases", count(progress.bases)),
        if progress.complete {
            "Upload complete (sample not finalized)"
        } else {
            "Upload in progress"
        }
        .to_owned(),
    ]
}

/// Values shown in the interactive server startup banner.
#[cfg(feature = "hud")]
pub struct StartupBanner<'a> {
    /// Local listening address.
    pub(crate) address: &'a str,
    /// Sample name.
    pub(crate) sample: &'a str,
    /// Optional reference path.
    pub(crate) reference: Option<&'a str>,
    /// Sketch configuration summary.
    pub(crate) sketch: &'a str,
    /// Optional configuration file path.
    pub(crate) config: Option<&'a str>,
    /// Worker count and refresh interval in milliseconds.
    pub(crate) profiling: Option<(usize, u128)>,
}

#[cfg(feature = "hud")]
fn banner_row_start(output: &mut String, label: &str, width: usize, color: bool) {
    let arrow = "➜";
    if color {
        let _ = write!(output, "  {}  ", arrow.cyan());
    } else {
        let _ = write!(output, "  {arrow}  ");
    }
    let _ = write!(output, "{label:width$}  ");
}

#[cfg(feature = "hud")]
fn banner_row(
    output: &mut String,
    label: &str,
    width: usize,
    color: bool,
    prefix: &str,
    value: &str,
) {
    banner_row_start(output, label, width, color);
    output.push_str(prefix);
    for ch in value.chars() {
        if ch.is_control() {
            output.extend(ch.escape_default());
        } else {
            output.push(ch);
        }
    }
    output.push('\n');
}

#[cfg(feature = "hud")]
fn startup_banner(info: &StartupBanner<'_>, color: bool) -> String {
    let heading = format!("AXYLPH v{}  ready", env!("CARGO_PKG_VERSION"));
    let mut output = if color {
        format!("\n  {}\n\n", heading.green().bold())
    } else {
        format!("\n  {heading}\n\n")
    };
    let width = if info.reference.is_some() { 10 } else { 7 };
    banner_row(&mut output, "Local:", width, color, "http://", info.address);
    banner_row(&mut output, "Sample:", width, color, "", info.sample);
    banner_row(
        &mut output,
        "Mode:",
        width,
        color,
        "",
        if info.profiling.is_some() {
            "profiling"
        } else {
            "sketch only"
        },
    );
    if let Some(reference) = info.reference {
        banner_row(&mut output, "Reference:", width, color, "", reference);
    }
    banner_row(&mut output, "Sketch:", width, color, "", info.sketch);
    banner_row(
        &mut output,
        "Config:",
        width,
        color,
        "",
        info.config.unwrap_or("defaults (no file loaded)"),
    );
    if let Some((workers, interval)) = info.profiling {
        banner_row_start(&mut output, "Workers:", width, color);
        let _ = writeln!(output, "{workers}");
        banner_row_start(&mut output, "Refresh:", width, color);
        let _ = writeln!(output, "{interval} ms");
    }
    let hint = "Ctrl-C to stop / state is not saved";
    if color {
        let _ = writeln!(output, "\n  {}", hint.dimmed());
    } else {
        let _ = writeln!(output, "\n  {hint}");
    }
    output
}

#[cfg(feature = "hud")]
impl Display {
    /// Prints the interactive server startup banner.
    pub(crate) fn startup(&self, info: &StartupBanner<'_>) {
        if let Some(bar) = &self.bar {
            let color = std::env::var_os("NO_COLOR").is_none();
            bar.suspend(|| eprint!("{}", startup_banner(info, color)));
        }
    }
}

impl Drop for Display {
    fn drop(&mut self) {
        #[cfg(feature = "hud")]
        if let Some(bar) = &self.bar {
            bar.finish_with_message(self.final_message());
        }
    }
}

fn sanitize(message: &str) -> String {
    let mut safe = String::new();
    for ch in message.chars() {
        match ch {
            '\n' => safe.push('\n'),
            '\t' => safe.push_str("    "),
            ch if ch.is_control() => safe.extend(ch.escape_default()),
            ch => safe.push(ch),
        }
    }
    safe
}

fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut output = String::new();
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && digits.len().saturating_sub(index) % 3 == 0 {
            output.push(',');
        }
        output.push(ch);
    }
    output
}

/// Builds the server's current status lines.
pub fn server_lines(
    reads: u64,
    bases: u64,
    observation: Option<&Observation>,
    profiling: bool,
    error: bool,
    listening: bool,
) -> Vec<String> {
    let admission = if !listening
        || observation
            .is_some_and(|o| o.state() != SessionState::Open || o.ingestion_error().is_some())
        || error
    {
        "closed"
    } else {
        "open"
    };
    let (profile, coverage) = if !profiling {
        ("disabled", None)
    } else if let Some(obs) = observation {
        if let Some(Ok(snapshot)) = obs.finish_outcome() {
            ("final", Some(snapshot.checkpoint().reads()))
        } else if obs.finish_outcome().is_some_and(Result::is_err) {
            ("finish failed", None)
        } else if let Some(snapshot) = obs.snapshot() {
            ("available", Some(snapshot.checkpoint().reads()))
        } else {
            ("awaiting", None)
        }
    } else {
        ("awaiting", None)
    };
    vec![
        "Processed".to_owned(),
        format!("{} reads", count(reads)),
        format!("{} bases", count(bases)),
        format!("Admission: {admission}"),
        format!("Profile: {profile}"),
        coverage.map_or_else(String::new, |reads| {
            format!("Covers: {} reads", count(reads))
        }),
        if listening {
            "Server: listening"
        } else {
            "Server: stopped"
        }
        .to_owned(),
    ]
}

#[cfg(test)]
mod tests {
    use super::{count, sanitize, server_lines};

    #[cfg(feature = "hud")]
    #[test]
    fn skipped_upload_progress_is_retained_for_drop_and_completion() {
        let mut display = super::Display::new(crate::config::DisplayMode::Plain);
        let first = axylph::Progress {
            reads: 1,
            bases: 10,
            complete: false,
        };
        let latest = axylph::Progress {
            reads: 2,
            bases: 20,
            complete: false,
        };
        display.upload_progress(first, true);
        display.upload_progress(latest, false);
        assert_eq!(
            display.final_message(),
            "2 reads valid\n20 bases\nUpload in progress"
        );

        let complete = axylph::Progress {
            complete: true,
            ..latest
        };
        display.upload_progress(complete, true);
        assert_eq!(
            display.final_message(),
            "2 reads valid\n20 bases\nUpload complete (sample not finalized)"
        );
        assert!(display.pending_progress.is_none());
    }

    #[cfg(feature = "hud")]
    #[test]
    fn startup_banner_has_aligned_rows_and_escapes_values() {
        let banner = super::startup_banner(
            &super::StartupBanner {
                address: "127.0.0.1:1234",
                sample: "snow\n\u{1b}[6n",
                reference: None,
                sketch: "c=3, k=31, dedup=true",
                config: None,
                profiling: None,
            },
            false,
        );
        assert!(banner.contains(concat!(
            "  AXYLPH v",
            env!("CARGO_PKG_VERSION"),
            "  ready\n"
        )));
        assert!(banner.contains("  ➜  Local:   http://127.0.0.1:1234\n"));
        assert!(banner.contains("  ➜  Sample:  snow\\n\\u{1b}[6n\n"));
        assert!(banner.contains("  ➜  Mode:    sketch only\n"));
        assert!(banner.contains("  ➜  Config:  defaults (no file loaded)\n"));
        assert!(!banner.contains("Reference:"));
        assert!(banner.contains("Ctrl-C to stop"));
    }

    #[cfg(feature = "hud")]
    #[test]
    fn startup_banner_reports_profiling_settings() {
        let banner = super::startup_banner(
            &super::StartupBanner {
                address: "[::1]:42",
                sample: "sample",
                reference: Some("/long/reference"),
                sketch: "c=5, k=21, dedup=false",
                config: Some("/config.toml"),
                profiling: Some((4, 500)),
            },
            false,
        );
        assert!(banner.contains("  ➜  Mode:       profiling\n"));
        assert!(banner.contains("  ➜  Reference:  /long/reference\n"));
        assert!(banner.contains("  ➜  Workers:    4\n"));
        assert!(banner.contains("  ➜  Refresh:    500 ms\n"));
        assert!(banner.contains("  ➜  Config:     /config.toml\n"));
    }

    #[test]
    fn exact_counts_and_profile_states() {
        assert_eq!(count(u64::MAX), "18,446,744,073,709,551,615");
        let disabled = server_lines(1234, 5678, None, false, false, true).join(" ");
        assert!(disabled.contains("1,234 reads"));
        assert!(disabled.contains("5,678 bases"));
        assert!(disabled.contains("Profile: disabled"));
        let awaiting = server_lines(0, 0, None, true, false, false).join(" ");
        assert!(awaiting.contains("Admission: closed"));
        assert!(awaiting.contains("Profile: awaiting"));
        assert!(awaiting.contains("Server: stopped"));
        let finished = server_lines(0, 0, None, true, true, true).join(" ");
        assert!(finished.contains("Admission: closed"));
        assert!(finished.contains("Server: listening"));
    }

    #[test]
    fn largest_counts_fit_a_forty_column_terminal() {
        let lines = server_lines(u64::MAX, u64::MAX, None, true, false, true);
        assert!(lines.iter().all(|line| line.len() <= 40));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("18,446,744,073,709,551,615 reads"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("18,446,744,073,709,551,615 bases"))
        );
    }

    #[test]
    fn diagnostics_escape_controls_and_keep_unicode_and_newlines() {
        assert_eq!(
            sanitize("雪\tpath\n\u{1b}[6n\r"),
            "雪    path\n\\u{1b}[6n\\r"
        );
    }
}
