use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use compact_str::CompactString;
use rustix::process::{Pid, Signal, kill_process, test_kill_process};
#[cfg(target_os = "linux")]
use {
    rustix::fd::OwnedFd,
    rustix::process::{PidfdFlags, pidfd_open, pidfd_send_signal},
    tokio::io::Interest,
    tokio::io::unix::AsyncFd,
};

use crate::PgError;

/// Parsed representation of `$PGDATA/postmaster.pid` (`src/include/miscadmin.h`).
///
/// # Examples
///
/// ```
/// use negroni_pg::pidfd_monitor::PostmasterPidFile;
///
/// let sample = "4242\n/var/lib/postgresql/17/main\n1731400000\n5432\n/var/run/postgresql\n*\n  5432001    0\nready   \n";
/// let parsed = PostmasterPidFile::parse_str(sample)?;
/// assert_eq!(parsed.pid, 4242);
/// assert_eq!(parsed.port, 5432);
/// assert_eq!(parsed.status.as_deref(), Some("ready"));
/// # Ok::<(), negroni_pg::PgError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostmasterPidFile {
    pub pid: u32,
    pub data_dir: PathBuf,
    pub start_epoch_secs: i64,
    pub port: u16,
    pub socket_dir: Option<PathBuf>,
    pub listen_addr: CompactString,
    pub status: Option<CompactString>,
}

impl PostmasterPidFile {
    pub fn read_from_pgdata(pgdata: &Path) -> Result<Option<Self>, PgError> {
        let pid_path = pgdata.join("postmaster.pid");
        match fs::read_to_string(&pid_path) {
            Ok(contents) => Self::parse_str(&contents).map(Some),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(PgError::Io {
                context: format!("reading '{}'", pid_path.display()),
                source,
            }),
        }
    }

    pub fn parse_str(contents: &str) -> Result<Self, PgError> {
        let lines: Vec<&str> = contents.lines().collect();
        let pid_line = lines.first().ok_or_else(|| {
            PgError::InvalidMetadata("postmaster.pid is empty (missing PID line)".to_owned())
        })?;
        let raw_pid: i32 = pid_line.trim().parse().map_err(|_| {
            PgError::InvalidMetadata(format!("invalid PID '{pid_line}' in postmaster.pid"))
        })?;
        if raw_pid <= 0 {
            return Err(PgError::InvalidMetadata(format!(
                "negative or zero PID ({raw_pid}) in postmaster.pid indicates single-user mode"
            )));
        }
        let pid = u32::try_from(raw_pid).map_err(|_| {
            PgError::InvalidMetadata(format!("out-of-range PID ({raw_pid}) in postmaster.pid"))
        })?;

        let data_dir = lines
            .get(1)
            .map(|s| PathBuf::from(s.trim()))
            .unwrap_or_default();

        let start_epoch_secs = lines
            .get(2)
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);

        let port = lines
            .get(3)
            .and_then(|s| s.trim().parse::<u16>().ok())
            .unwrap_or(5432);

        let socket_dir = lines
            .get(4)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        let listen_addr = lines
            .get(5)
            .map(|s| CompactString::from(s.trim()))
            .unwrap_or_default();

        let status = lines
            .get(7)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(CompactString::from);

        Ok(Self {
            pid,
            data_dir,
            start_epoch_secs,
            port,
            socket_dir,
            listen_addr,
            status,
        })
    }
}

/// Parses process state character (`R`, `S`, `D`, `Z`, ...) and monotonic `starttime` clock ticks
/// (field 22) from a Linux `/proc/<pid>/stat` line, safely skipping arbitrary parentheses in `comm`.
pub fn parse_proc_stat(stat_line: &str) -> Option<(char, u64)> {
    let close_paren = stat_line.rfind(')')?;
    let after_comm = stat_line.get(close_paren + 1..)?.trim_start();
    let mut fields = after_comm.split_whitespace();
    let state_char = fields.next()?.chars().next()?;
    // Field 3 is `state` (`fields.next()` above); field 22 (`starttime`) is 19 fields further (index 18).
    let start_ticks_str = fields.nth(18)?;
    let start_ticks = start_ticks_str.parse::<u64>().ok()?;
    Some((state_char, start_ticks))
}

#[cfg(target_os = "linux")]
fn verify_linux_process_matches_pgdata(pid: u32, expected_pgdata: Option<&Path>) -> Option<u64> {
    let stat_raw = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (state_char, start_ticks) = parse_proc_stat(&stat_raw)?;
    if matches!(state_char, 'Z' | 'X' | 'x') {
        return None;
    }

    if let Some(pgdata) = expected_pgdata {
        let expected_canon = fs::canonicalize(pgdata).unwrap_or_else(|_| pgdata.to_path_buf());
        let cwd_matches = fs::read_link(format!("/proc/{pid}/cwd"))
            .ok()
            .and_then(|cwd| fs::canonicalize(&cwd).ok().or(Some(cwd)))
            .is_some_and(|cwd| cwd == expected_canon);

        let cmdline_matches = fs::read(format!("/proc/{pid}/cmdline"))
            .ok()
            .is_some_and(|bytes| {
                let text = String::from_utf8_lossy(&bytes);
                text.contains("postgres") || text.contains("postmaster")
            });

        if !cwd_matches && !cmdline_matches {
            return None;
        }
    }

    Some(start_ticks)
}

#[derive(Debug)]
enum MonitorBackend {
    #[cfg(target_os = "linux")]
    PidFd(AsyncFd<OwnedFd>),
    PosixPoll,
}

/// Race-free kernel handle monitoring an adopted or spawned PostgreSQL `postmaster` process.
///
/// On Linux 5.3+, wraps a `PIDFD_NONBLOCK` kernel file descriptor inside `tokio::io::unix::AsyncFd`
/// so process termination wakes Tokio directly without polling, and `send_signal` uses
/// `pidfd_send_signal` so recycled PIDs can never receive `SIGINT` or `SIGQUIT`.
#[derive(Debug)]
pub struct PidfdProcessMonitor {
    pid: Pid,
    raw_pid: u32,
    start_ticks: Option<u64>,
    backend: MonitorBackend,
}

impl PidfdProcessMonitor {
    /// Adopts an existing `pid`, optionally verifying that `/proc/<pid>` belongs to the PostgreSQL
    /// instance rooted at `expected_pgdata`. Returns `Ok(None)` if the process no longer exists,
    /// is a zombie, or belongs to a recycled unrelated binary.
    pub fn adopt(raw_pid: u32, expected_pgdata: Option<&Path>) -> Result<Option<Self>, PgError> {
        let pid_i32 = i32::try_from(raw_pid).map_err(|_| {
            PgError::InvalidMetadata(format!("PID {raw_pid} exceeds positive i32 range"))
        })?;
        let Some(pid) = Pid::from_raw(pid_i32) else {
            return Ok(None);
        };

        #[cfg(target_os = "linux")]
        {
            let Some(initial_ticks) = verify_linux_process_matches_pgdata(raw_pid, expected_pgdata)
            else {
                return Ok(None);
            };

            match pidfd_open(pid, PidfdFlags::NONBLOCK) {
                Ok(owned_fd) => {
                    // Re-verify `/proc/<pid>/stat` start_ticks AFTER opening `pidfd` so even a
                    // nanosecond PID reuse race between `/proc` inspection and `pidfd_open` is caught!
                    let Some(post_open_ticks) =
                        verify_linux_process_matches_pgdata(raw_pid, expected_pgdata)
                    else {
                        return Ok(None);
                    };
                    if post_open_ticks != initial_ticks {
                        return Ok(None);
                    }

                    // `owned_fd` is a `std::os::fd::OwnedFd` with exclusive I/O-safe ownership
                    // (closed only on Drop, never exchanged via `get_mut`), satisfying Tokio PR #8587
                    // while keeping `negroni-pg` strictly `#![forbid(unsafe_code)]`.
                    #[allow(deprecated)]
                    let async_fd =
                        AsyncFd::with_interest(owned_fd, Interest::READABLE).map_err(|source| {
                            PgError::Io {
                                context: format!("registering AsyncFd for pidfd {raw_pid}"),
                                source,
                            }
                        })?;
                    return Ok(Some(Self {
                        pid,
                        raw_pid,
                        start_ticks: Some(post_open_ticks),
                        backend: MonitorBackend::PidFd(async_fd),
                    }));
                }
                Err(err) if err == rustix::io::Errno::SRCH => return Ok(None),
                Err(err) => {
                    tracing::debug!(
                        pid = raw_pid,
                        errno = %err,
                        "pidfd_open unavailable; falling back to POSIX kill(pid, 0) monitor"
                    );
                }
            }

            if test_kill_process(pid).is_err() {
                return Ok(None);
            }
            Ok(Some(Self {
                pid,
                raw_pid,
                start_ticks: Some(initial_ticks),
                backend: MonitorBackend::PosixPoll,
            }))
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = expected_pgdata;
            match test_kill_process(pid) {
                Ok(()) => Ok(Some(Self {
                    pid,
                    raw_pid,
                    start_ticks: None,
                    backend: MonitorBackend::PosixPoll,
                })),
                Err(err) if err == rustix::io::Errno::SRCH => Ok(None),
                Err(err) => Err(PgError::Io {
                    context: format!("probing process {raw_pid}"),
                    source: std::io::Error::from(err),
                }),
            }
        }
    }

    pub const fn raw_pid(&self) -> u32 {
        self.raw_pid
    }

    pub const fn start_ticks(&self) -> Option<u64> {
        self.start_ticks
    }

    /// Checks whether the monitored process is still alive right now without blocking.
    pub fn is_alive(&self) -> bool {
        match &self.backend {
            #[cfg(target_os = "linux")]
            MonitorBackend::PidFd(async_fd) => {
                // `pidfd_send_signal(fd, ...)` isn't needed just to probe; checking `/proc/<pid>/stat`
                // zombie state + `test_kill_process` while verifying `start_ticks` is non-blocking.
                let _ = async_fd;
                match fs::read_to_string(format!("/proc/{}/stat", self.raw_pid)) {
                    Ok(stat_raw) => match parse_proc_stat(&stat_raw) {
                        Some((state, ticks)) => {
                            !matches!(state, 'Z' | 'X' | 'x')
                                && self.start_ticks.is_none_or(|expected| expected == ticks)
                        }
                        None => false,
                    },
                    Err(_) => false,
                }
            }
            MonitorBackend::PosixPoll => test_kill_process(self.pid).is_ok(),
        }
    }

    /// Sends `sig` (`Signal::INT`, `Signal::QUIT`, `Signal::HUP`) to the monitored postmaster.
    /// Returns `Ok(false)` if the process has already exited (`ESRCH`).
    pub fn send_signal(&self, sig: Signal) -> Result<bool, PgError> {
        match &self.backend {
            #[cfg(target_os = "linux")]
            MonitorBackend::PidFd(async_fd) => match pidfd_send_signal(async_fd.get_ref(), sig) {
                Ok(()) => Ok(true),
                Err(err) if err == rustix::io::Errno::SRCH => Ok(false),
                Err(err) => Err(PgError::Io {
                    context: format!("pidfd_send_signal({sig:?}) to PID {}", self.raw_pid),
                    source: std::io::Error::from(err),
                }),
            },
            MonitorBackend::PosixPoll => {
                #[cfg(target_os = "linux")]
                if let Some(expected_ticks) = self.start_ticks {
                    let still_same = fs::read_to_string(format!("/proc/{}/stat", self.raw_pid))
                        .ok()
                        .and_then(|s| parse_proc_stat(&s))
                        .is_some_and(|(_, ticks)| ticks == expected_ticks);
                    if !still_same {
                        return Ok(false);
                    }
                }
                match kill_process(self.pid, sig) {
                    Ok(()) => Ok(true),
                    Err(err) if err == rustix::io::Errno::SRCH => Ok(false),
                    Err(err) => Err(PgError::Io {
                        context: format!("kill_process({sig:?}) to PID {}", self.raw_pid),
                        source: std::io::Error::from(err),
                    }),
                }
            }
        }
    }

    /// Awaits process termination (using kernel `pidfd` readability on Linux or `500ms` POSIX poll).
    /// This method is cancel-safe when awaited inside a background task or timeout wrapper.
    pub async fn wait_for_exit(&self) -> Result<(), PgError> {
        match &self.backend {
            #[cfg(target_os = "linux")]
            MonitorBackend::PidFd(async_fd) => {
                let mut guard = async_fd.readable().await.map_err(|source| PgError::Io {
                    context: format!("waiting on pidfd readability for PID {}", self.raw_pid),
                    source,
                })?;
                guard.retain_ready();
                Ok(())
            }
            MonitorBackend::PosixPoll => {
                let mut ticker = tokio::time::interval(Duration::from_millis(500));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                while self.is_alive() {
                    ticker.tick().await;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use tokio::process::Command;

    use super::*;

    #[test]
    fn parses_proc_stat_even_with_spaces_and_parens_in_comm() {
        let sample = "19428 (postgres (main) worker) S 1 19428 19428 0 -1 4194560 120 0 0 0 4 2 0 0 20 0 1 0 987654321 25165824 1024";
        let (state, ticks) = parse_proc_stat(sample).unwrap();
        assert_eq!(state, 'S');
        assert_eq!(ticks, 987_654_321);
    }

    #[test]
    fn rejects_single_user_negative_pid_in_postmaster_pid() {
        let single_user = "-12345\n/pgdata\n1731400000\n5432\n";
        assert!(PostmasterPidFile::parse_str(single_user).is_err());
    }

    #[tokio::test]
    async fn pidfd_monitor_adopts_child_sends_signal_and_wakes_on_exit() {
        let mut child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();

        let monitor = PidfdProcessMonitor::adopt(pid, None).unwrap().unwrap();
        assert_eq!(monitor.raw_pid(), pid);
        assert!(monitor.is_alive());

        // Terminate via pidfd_send_signal(SIGINT) and reap child so kernel marks pidfd readable.
        assert!(monitor.send_signal(Signal::INT).unwrap());
        let reap_fut = child.wait();
        let wait_fut = monitor.wait_for_exit();
        let (reap_res, exit_res) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(reap_fut, wait_fut)
        })
        .await
        .unwrap();
        assert!(reap_res.is_ok());
        assert!(exit_res.is_ok());
        assert!(!monitor.is_alive());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pidfd_identity_guard_rejects_unrelated_process_for_pgdata() {
        let fake_pgdata =
            std::env::temp_dir().join(format!("negroni-pidfd-guard-{}", std::process::id()));
        fs::create_dir_all(&fake_pgdata).unwrap();

        // Spawn `sleep` in `/` so neither cwd nor cmdline (`postgres`/`postmaster`) matches `fake_pgdata`.
        let mut child = Command::new("sleep")
            .arg("10")
            .current_dir("/")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();

        let adopted = PidfdProcessMonitor::adopt(pid, Some(&fake_pgdata)).unwrap();
        assert!(
            adopted.is_none(),
            "unrelated binary with mismatched cwd/cmdline must not be adopted as postmaster"
        );

        let _ = child.kill().await;
        let _ = fs::remove_dir_all(&fake_pgdata);
    }
}
