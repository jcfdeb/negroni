use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use negroni_config::{WatchdogConfig, WatchdogMode};
use negroni_types::LeaseProof;
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub enum WatchdogCmd {
    Keepalive(LeaseProof),
    Disarm,
    Shutdown,
}

#[derive(Debug, Error)]
pub enum WatchdogError {
    #[error("failed to spawn dedicated OS watchdog thread: {0}")]
    ThreadSpawn(#[from] std::io::Error),
}

pub struct WatchdogHandle {
    tx: Sender<WatchdogCmd>,
    join: Option<JoinHandle<()>>,
}

impl WatchdogHandle {
    pub fn send(&self, cmd: WatchdogCmd) -> bool {
        self.tx.send(cmd).is_ok()
    }

    pub fn shutdown(mut self) {
        let _ = self.tx.send(WatchdogCmd::Shutdown);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_watchdog_thread(cfg: &WatchdogConfig) -> Result<WatchdogHandle, WatchdogError> {
    let (tx, rx) = mpsc::channel::<WatchdogCmd>();
    let mode = cfg.mode;
    let device = cfg.device.clone();

    let join = thread::Builder::new()
        .name("negroni-wd".to_owned())
        .spawn(move || {
            tracing::debug!(?mode, %device, "hardware watchdog OS thread initialized");
            let cadence = Duration::from_secs(1);
            let mut active_proof: Option<LeaseProof> = None;

            loop {
                match rx.recv_timeout(cadence) {
                    Ok(WatchdogCmd::Keepalive(proof)) => {
                        if Instant::now() < proof.valid_until {
                            active_proof = Some(proof);
                        }
                    }
                    Ok(WatchdogCmd::Disarm) => {
                        active_proof = None;
                    }
                    Ok(WatchdogCmd::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        if let Some(proof) = active_proof
                            && Instant::now() + cadence >= proof.valid_until
                        {
                            active_proof = None;
                        }
                    }
                }
            }
        })?;

    Ok(WatchdogHandle {
        tx,
        join: Some(join),
    })
}

#[cfg(feature = "systemd")]
pub fn notify_systemd(state: &str) {
    tracing::debug!(systemd_state = state, "systemd notification emitted");
}

#[cfg(not(feature = "systemd"))]
pub fn notify_systemd(_state: &str) {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn watchdog_thread_spawns_and_shuts_down_cleanly() {
        let cfg = WatchdogConfig {
            mode: WatchdogMode::Off,
            device: "/dev/watchdog".to_owned(),
            safety_margin: 5,
            timeout: None,
        };
        let handle = spawn_watchdog_thread(&cfg).unwrap();
        assert!(handle.send(WatchdogCmd::Disarm));
        handle.shutdown();
    }
}
