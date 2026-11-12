#![allow(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WatchdogArmError {
    #[error("failed to open watchdog device '{device}': {reason}")]
    DeviceOpen { device: String, reason: String },
    #[error("watchdog WDIOC_SETTIMEOUT ioctl failed on '{device}': {reason}")]
    IoctlFailed { device: String, reason: String },
    #[error(
        "kernel rounded watchdog timeout from {requested:?} to {actual:?}, violating safe fencing window [{min_required:?}, {ttl:?})"
    )]
    InvalidKernelGranularity {
        requested: Duration,
        actual: Duration,
        min_required: Duration,
        ttl: Duration,
    },
    #[error("hardware watchdog ioctl is only supported on Linux hosts")]
    UnsupportedPlatform,
    #[error("watchdog OS thread is disconnected")]
    ThreadDisconnected,
}

pub trait ArmedWatchdog: Send {
    fn keepalive(&mut self) -> io::Result<()>;
    fn disarm(self: Box<Self>) -> io::Result<()>;
}

pub trait WatchdogDevice: Send + Sync + 'static {
    fn arm(
        &self,
        requested: Duration,
    ) -> Result<(Box<dyn ArmedWatchdog>, Duration), WatchdogArmError>;
}

/// Arms `device` and verifies the kernel-returned timeout `T_wd_actual` satisfies
/// `min_required <= T_wd_actual < ttl`. If the driver rounds outside this window,
/// writes magic `'V'` immediately before closing the descriptor so the host is not reset.
pub fn validate_and_arm(
    device: &dyn WatchdogDevice,
    requested: Duration,
    min_required: Duration,
    ttl: Duration,
) -> Result<(Box<dyn ArmedWatchdog>, Duration), WatchdogArmError> {
    let (armed, actual) = device.arm(requested)?;
    if actual < min_required || actual >= ttl {
        let _ = armed.disarm();
        return Err(WatchdogArmError::InvalidKernelGranularity {
            requested,
            actual,
            min_required,
            ttl,
        });
    }
    Ok((armed, actual))
}

#[derive(Debug, Clone)]
pub struct KernelWatchdogDevice {
    device_path: String,
}

impl KernelWatchdogDevice {
    pub fn new(device_path: impl Into<String>) -> Self {
        Self {
            device_path: device_path.into(),
        }
    }
}

#[cfg(target_os = "linux")]
mod linux_sys {
    use std::fs::File;
    use std::io;

    use rustix::ioctl::{self, Getter, Opcode, Updater};

    // Linux `include/uapi/linux/watchdog.h`:
    //   #define WATCHDOG_IOCTL_BASE 'W'
    //   #define WDIOC_KEEPALIVE     _IOR(WATCHDOG_IOCTL_BASE, 5, int)
    //   #define WDIOC_SETTIMEOUT    _IOWR(WATCHDOG_IOCTL_BASE, 6, int)
    const WDIOC_KEEPALIVE: Opcode = ioctl::opcode::read::<i32>(b'W', 5);
    const WDIOC_SETTIMEOUT: Opcode = ioctl::opcode::read_write::<i32>(b'W', 6);

    pub(super) fn set_timeout_secs(file: &File, requested_secs: i32) -> io::Result<i32> {
        let mut secs = requested_secs;
        // SAFETY: `WDIOC_SETTIMEOUT` (`_IOWR('W', 6, int)`) matches the Linux kernel
        // `include/uapi/linux/watchdog.h` ABI: it reads an aligned `i32` timeout in seconds
        // from `&mut secs` and writes the driver's actual programmed timeout back into `secs`
        // over the valid open watchdog file descriptor `file`.
        unsafe {
            let ctl = Updater::<WDIOC_SETTIMEOUT, i32>::new(&mut secs);
            ioctl::ioctl(file, ctl).map_err(io::Error::from)?;
        }
        Ok(secs)
    }

    pub(super) fn send_keepalive(file: &File) -> io::Result<()> {
        // SAFETY: `WDIOC_KEEPALIVE` (`_IOR('W', 5, int)`) matches the Linux kernel watchdog
        // ABI: `Getter` allocates an aligned `MaybeUninit<i32>` output slot on the stack for
        // the `ioctl` call on the valid open watchdog file descriptor `file`.
        unsafe {
            let ctl = Getter::<WDIOC_KEEPALIVE, i32>::new();
            let _ = ioctl::ioctl(file, ctl).map_err(io::Error::from)?;
        }
        Ok(())
    }
}

struct KernelArmedFile {
    file: File,
}

impl ArmedWatchdog for KernelArmedFile {
    fn keepalive(&mut self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            linux_sys::send_keepalive(&self.file)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Linux /dev/watchdog unavailable",
            ))
        }
    }

    fn disarm(mut self: Box<Self>) -> io::Result<()> {
        // Magic Close feature in Linux watchdog API: writing `'V'` before closing disarms timer.
        self.file.write_all(b"V")?;
        self.file.flush()
    }
}

impl WatchdogDevice for KernelWatchdogDevice {
    fn arm(
        &self,
        requested: Duration,
    ) -> Result<(Box<dyn ArmedWatchdog>, Duration), WatchdogArmError> {
        #[cfg(target_os = "linux")]
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.device_path)
                .map_err(|e| WatchdogArmError::DeviceOpen {
                    device: self.device_path.clone(),
                    reason: e.to_string(),
                })?;

            let req_secs = i32::try_from(requested.as_secs()).unwrap_or(i32::MAX);
            let mut armed = Box::new(KernelArmedFile { file });
            match linux_sys::set_timeout_secs(&armed.file, req_secs) {
                Ok(actual_secs) => {
                    let actual_u64 = u64::try_from(actual_secs.max(0)).unwrap_or(0);
                    // Immediately issue an initial keepalive after programming timeout.
                    let _ = armed.keepalive();
                    Ok((armed, Duration::from_secs(actual_u64)))
                }
                Err(err) => {
                    let _ = armed.disarm();
                    Err(WatchdogArmError::IoctlFailed {
                        device: self.device_path.clone(),
                        reason: err.to_string(),
                    })
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = requested;
            Err(WatchdogArmError::UnsupportedPlatform)
        }
    }
}

/// Lock-free atomic telemetry for deterministic software watchdog testing.
#[derive(Debug, Default)]
struct MockState {
    armed: AtomicBool,
    arm_count: AtomicU64,
    keepalive_count: AtomicU64,
    disarm_magic_v_count: AtomicU64,
    rounded_timeout_secs: AtomicU64,
    fail_open: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MockWatchdogSnapshot {
    pub is_armed: bool,
    pub arm_count: u64,
    pub keepalive_count: u64,
    pub disarm_magic_v_count: u64,
}

/// Software mock implementation of `/dev/watchdog` for unit and simulation tests.
#[derive(Debug, Clone, Default)]
pub struct SoftwareMockWatchdog {
    state: Arc<MockState>,
}

impl SoftwareMockWatchdog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates a hardware watchdog driver that snaps `WDIOC_SETTIMEOUT` to `actual_secs`.
    pub fn with_driver_rounded_timeout(actual_secs: u64) -> Self {
        let mock = Self::new();
        mock.state
            .rounded_timeout_secs
            .store(actual_secs, Ordering::SeqCst);
        mock
    }

    /// Simulates missing `/dev/watchdog` or `EACCES` permission failure.
    pub fn failing_open() -> Self {
        let mock = Self::new();
        mock.state.fail_open.store(true, Ordering::SeqCst);
        mock
    }

    pub fn snapshot(&self) -> MockWatchdogSnapshot {
        MockWatchdogSnapshot {
            is_armed: self.state.armed.load(Ordering::SeqCst),
            arm_count: self.state.arm_count.load(Ordering::SeqCst),
            keepalive_count: self.state.keepalive_count.load(Ordering::SeqCst),
            disarm_magic_v_count: self.state.disarm_magic_v_count.load(Ordering::SeqCst),
        }
    }
}

struct MockArmedHandle {
    state: Arc<MockState>,
}

impl ArmedWatchdog for MockArmedHandle {
    fn keepalive(&mut self) -> io::Result<()> {
        self.state.keepalive_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn disarm(self: Box<Self>) -> io::Result<()> {
        self.state.armed.store(false, Ordering::SeqCst);
        self.state
            .disarm_magic_v_count
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl WatchdogDevice for SoftwareMockWatchdog {
    fn arm(
        &self,
        requested: Duration,
    ) -> Result<(Box<dyn ArmedWatchdog>, Duration), WatchdogArmError> {
        if self.state.fail_open.load(Ordering::SeqCst) {
            return Err(WatchdogArmError::DeviceOpen {
                device: "mock://watchdog".to_owned(),
                reason: "simulated ENOENT / EACCES".to_owned(),
            });
        }
        self.state.armed.store(true, Ordering::SeqCst);
        self.state.arm_count.fetch_add(1, Ordering::SeqCst);
        self.state.keepalive_count.fetch_add(1, Ordering::SeqCst);

        let override_secs = self.state.rounded_timeout_secs.load(Ordering::SeqCst);
        let actual = if override_secs > 0 {
            Duration::from_secs(override_secs)
        } else {
            requested
        };

        Ok((
            Box::new(MockArmedHandle {
                state: Arc::clone(&self.state),
            }),
            actual,
        ))
    }
}
