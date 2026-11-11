use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use negroni_config::{WatchdogConfig, WatchdogMode};
use negroni_types::LeaseProof;

use crate::ioctl_linux::{ArmedWatchdog, WatchdogArmError, WatchdogDevice, validate_and_arm};

#[derive(Debug)]
pub enum WatchdogCmd {
    /// Arms `/dev/watchdog` (if not already armed) and feeds a pre-flight lease proof,
    /// replying with the validated kernel hardware timeout (or `None` if `Off` / degraded `Automatic`).
    ArmAndFeed {
        proof: LeaseProof,
        reply: SyncSender<Result<Option<Duration>, WatchdogArmError>>,
    },
    /// Updates the monotonic `LeaseProof` and issues `WDIOC_KEEPALIVE` if valid.
    Keepalive(LeaseProof),
    /// Writes magic `'V'` and closes `/dev/watchdog`. Must ONLY be sent after PostgreSQL stops accepting writes.
    Disarm,
    /// Disarms any active device cleanly on daemon exit and terminates the OS thread.
    Shutdown,
}

struct ActiveSession {
    device: Box<dyn ArmedWatchdog>,
    actual_timeout: Duration,
    epoch: u64,
    valid_until: Instant,
}

/// Runs exclusively on the pre-allocated `negroni-wd` OS thread.
/// Invariant: ZERO async tasks, ZERO network/disk log I/O (`tracing` forbidden), ZERO unbounded allocations.
pub(crate) fn watchdog_thread_main(
    cfg: WatchdogConfig,
    backend: Box<dyn WatchdogDevice>,
    cadence: Duration,
    rx: Receiver<WatchdogCmd>,
) {
    let mut active: Option<ActiveSession> = None;

    loop {
        match rx.recv_timeout(cadence) {
            Ok(WatchdogCmd::ArmAndFeed { proof, reply }) => {
                let outcome = handle_arm_and_feed(&cfg, &*backend, &mut active, proof);
                let _ = reply.send(outcome);
            }
            Ok(WatchdogCmd::Keepalive(proof)) => {
                let _ = handle_arm_and_feed(&cfg, &*backend, &mut active, proof);
            }
            Ok(WatchdogCmd::Disarm) => {
                if let Some(session) = active.take() {
                    let _ = session.device.disarm();
                }
            }
            Ok(WatchdogCmd::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                if let Some(session) = active.take() {
                    let _ = session.device.disarm();
                }
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Some(ref mut session) = active {
                    // Pre-flight anchor rule: only ping hardware while an entire keepalive cadence
                    // still fits strictly inside `proof.valid_until`. Otherwise stop pinging and
                    // keep the fd open so hardware resets the host prior to `ttl`.
                    if Instant::now() + cadence < session.valid_until {
                        let _ = session.device.keepalive();
                    }
                }
            }
        }
    }
}

fn handle_arm_and_feed(
    cfg: &WatchdogConfig,
    backend: &dyn WatchdogDevice,
    active: &mut Option<ActiveSession>,
    proof: LeaseProof,
) -> Result<Option<Duration>, WatchdogArmError> {
    if cfg.mode == WatchdogMode::Off {
        return Ok(None);
    }

    let now = Instant::now();
    if now >= proof.valid_until {
        return Ok(active.as_ref().map(|s| s.actual_timeout));
    }

    if let Some(session) = active.as_mut() {
        if proof.epoch >= session.epoch {
            session.epoch = proof.epoch;
            session.valid_until = proof.valid_until;
            let _ = session.device.keepalive();
        }
        return Ok(Some(session.actual_timeout));
    }

    let requested = cfg
        .timeout
        .unwrap_or_else(|| cfg.ttl.saturating_sub(Duration::from_secs(5)));

    match validate_and_arm(backend, requested, cfg.min_timeout, cfg.ttl) {
        Ok((device, actual_timeout)) => {
            *active = Some(ActiveSession {
                device,
                actual_timeout,
                epoch: proof.epoch,
                valid_until: proof.valid_until,
            });
            Ok(Some(actual_timeout))
        }
        Err(err) => match cfg.mode {
            WatchdogMode::Required => Err(err),
            WatchdogMode::Automatic | WatchdogMode::Off => Ok(None),
        },
    }
}
