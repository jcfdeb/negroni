pub mod ioctl_linux;
pub mod systemd;
pub mod thread;

use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;
use std::time::Duration;

use negroni_config::WatchdogConfig;
use negroni_types::LeaseProof;
use thiserror::Error;

pub use crate::ioctl_linux::{
    ArmedWatchdog, KernelWatchdogDevice, MockWatchdogSnapshot, SoftwareMockWatchdog,
    WatchdogArmError, WatchdogDevice,
};
pub use crate::systemd::notify_systemd;
pub use crate::thread::WatchdogCmd;

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

    /// Synchronously arms `/dev/watchdog` (if needed), validates the kernel-returned timeout
    /// against `[loop_wait + retry_timeout + 2s, ttl)`, and feeds `proof`.
    /// Returns `Err` when `WatchdogMode::Required` cannot safely arm the hardware timer.
    pub fn arm_and_feed(&self, proof: LeaseProof) -> Result<Option<Duration>, WatchdogArmError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(WatchdogCmd::ArmAndFeed {
                proof,
                reply: reply_tx,
            })
            .map_err(|_| WatchdogArmError::ThreadDisconnected)?;
        reply_rx
            .recv()
            .map_err(|_| WatchdogArmError::ThreadDisconnected)?
    }

    pub fn keepalive(&self, proof: LeaseProof) -> bool {
        self.send(WatchdogCmd::Keepalive(proof))
    }

    pub fn disarm(&self) -> bool {
        self.send(WatchdogCmd::Disarm)
    }

    pub fn shutdown(mut self) {
        let _ = self.tx.send(WatchdogCmd::Shutdown);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_watchdog_thread(cfg: &WatchdogConfig) -> Result<WatchdogHandle, WatchdogError> {
    let device = Box::new(KernelWatchdogDevice::new(cfg.device.clone()));
    spawn_watchdog_thread_with_device(cfg, device, Duration::from_secs(1))
}

pub fn spawn_watchdog_thread_with_device(
    cfg: &WatchdogConfig,
    device: Box<dyn WatchdogDevice>,
    cadence: Duration,
) -> Result<WatchdogHandle, WatchdogError> {
    let (tx, rx) = mpsc::channel::<WatchdogCmd>();
    let cfg_owned = cfg.clone();

    let join = std::thread::Builder::new()
        .name("negroni-wd".to_owned())
        .spawn(move || {
            thread::watchdog_thread_main(cfg_owned, device, cadence, rx);
        })?;

    Ok(WatchdogHandle {
        tx,
        join: Some(join),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::os::unix::net::UnixDatagram;
    use std::time::Instant;

    use negroni_config::WatchdogMode;

    use super::*;

    fn sample_cfg(mode: WatchdogMode) -> WatchdogConfig {
        WatchdogConfig {
            mode,
            device: "/dev/watchdog".to_owned(),
            safety_margin: 5,
            timeout: Some(Duration::from_secs(25)),
            min_timeout: Duration::from_secs(22),
            ttl: Duration::from_secs(30),
        }
    }

    #[test]
    fn watchdog_thread_spawns_and_shuts_down_cleanly() {
        let cfg = sample_cfg(WatchdogMode::Off);
        let handle = spawn_watchdog_thread(&cfg).unwrap();
        assert!(handle.disarm());
        handle.shutdown();
    }

    #[test]
    fn software_mock_arms_keeps_alive_and_writes_magic_v_on_disarm() {
        let mock = SoftwareMockWatchdog::new();
        let cfg = sample_cfg(WatchdogMode::Required);
        let handle = spawn_watchdog_thread_with_device(
            &cfg,
            Box::new(mock.clone()),
            Duration::from_millis(15),
        )
        .unwrap();

        let proof = LeaseProof {
            epoch: 1,
            valid_until: Instant::now() + Duration::from_secs(5),
        };
        let actual = handle.arm_and_feed(proof).unwrap();
        assert_eq!(actual, Some(Duration::from_secs(25)));

        std::thread::sleep(Duration::from_millis(45));
        let snap = mock.snapshot();
        assert!(snap.is_armed);
        assert_eq!(snap.arm_count, 1);
        assert!(snap.keepalive_count >= 2);
        assert_eq!(snap.disarm_magic_v_count, 0);

        assert!(handle.disarm());
        handle.shutdown();

        let final_snap = mock.snapshot();
        assert!(!final_snap.is_armed);
        assert_eq!(final_snap.disarm_magic_v_count, 1);
    }

    #[test]
    fn kernel_timeout_rounding_above_ttl_disarms_magic_v_and_rejects_required_mode() {
        // Simulate driver snapping 25s -> 32s (>= 30s TTL): must write 'V' immediately and fail Required!
        let mock = SoftwareMockWatchdog::with_driver_rounded_timeout(32);
        let req_cfg = sample_cfg(WatchdogMode::Required);
        let handle = spawn_watchdog_thread_with_device(
            &req_cfg,
            Box::new(mock.clone()),
            Duration::from_millis(20),
        )
        .unwrap();

        let proof = LeaseProof {
            epoch: 1,
            valid_until: Instant::now() + Duration::from_secs(10),
        };
        let err = handle.arm_and_feed(proof).unwrap_err();
        assert!(matches!(
            err,
            WatchdogArmError::InvalidKernelGranularity { .. }
        ));
        handle.shutdown();

        let snap = mock.snapshot();
        assert!(!snap.is_armed);
        assert_eq!(snap.disarm_magic_v_count, 1);

        // In Automatic mode, the same rounded hardware disarms safely and degrades to None.
        let auto_mock = SoftwareMockWatchdog::with_driver_rounded_timeout(32);
        let auto_cfg = sample_cfg(WatchdogMode::Automatic);
        let auto_handle = spawn_watchdog_thread_with_device(
            &auto_cfg,
            Box::new(auto_mock.clone()),
            Duration::from_millis(20),
        )
        .unwrap();
        assert_eq!(auto_handle.arm_and_feed(proof).unwrap(), None);
        auto_handle.shutdown();
        assert_eq!(auto_mock.snapshot().disarm_magic_v_count, 1);
    }

    #[test]
    fn systemd_notify_delivers_unix_datagram() {
        let sock_path =
            std::env::temp_dir().join(format!("negroni-sd-notify-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixDatagram::bind(&sock_path).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        systemd::send_to_notify_socket(sock_path.as_os_str(), b"READY=1").unwrap();

        let mut buf = [0u8; 32];
        let n = listener.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"READY=1");
        let _ = std::fs::remove_file(&sock_path);
    }
}
