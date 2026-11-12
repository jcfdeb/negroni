use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;

use crate::reducer::{HaEvent, JobKind};

/// Pluggable async executor for long-running PostgreSQL lifecycle jobs
/// (`initdb`, custom bootstrap scripts, `pg_basebackup`/`wal-g`/`pgbackrest`, and `pg_rewind`).
pub trait JobRunner: Send + Sync + 'static {
    fn run_job(
        &self,
        job: JobKind,
        target_conn: Option<String>,
        cancel_rx: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Default no-op runner used until `negroni-pg` subprocess runners attach in Phase 2.
#[derive(Debug, Default)]
pub struct NoopJobRunner;

impl JobRunner for NoopJobRunner {
    async fn run_job(
        &self,
        job: JobKind,
        _target_conn: Option<String>,
        _cancel_rx: watch::Receiver<bool>,
    ) -> Result<(), String> {
        Err(format!("job runner not yet attached for {job:?}"))
    }
}

struct ActiveJob {
    state_epoch: u64,
    job: JobKind,
    cancel_tx: watch::Sender<bool>,
    abort_handle: AbortHandle,
}

/// Manages at most one active background lifecycle job (`initdb`, clone, or `pg_rewind`),
/// enforcing cooperative + abortive cancellation and `state_epoch` fencing.
pub struct JobSupervisor<R: JobRunner> {
    runner: Arc<R>,
    event_tx: mpsc::Sender<HaEvent>,
    active: Option<ActiveJob>,
}

impl<R: JobRunner> JobSupervisor<R> {
    pub fn new(runner: Arc<R>, event_tx: mpsc::Sender<HaEvent>) -> Self {
        Self {
            runner,
            event_tx,
            active: None,
        }
    }

    pub fn active_job(&self) -> Option<(u64, JobKind)> {
        self.active.as_ref().map(|j| (j.state_epoch, j.job))
    }

    /// Cancels any previously running job and launches `job` on the pre-allocated Tokio pool.
    pub fn spawn(&mut self, state_epoch: u64, job: JobKind, target_conn: Option<String>) {
        self.cancel_all();

        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let runner = Arc::clone(&self.runner);
        let tx = self.event_tx.clone();
        let runner_cancel = cancel_rx.clone();

        let join_handle = tokio::spawn(async move {
            let outcome = tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    if changed.is_ok() && *cancel_rx.borrow() {
                        return;
                    }
                    Err("cancellation channel closed".to_owned())
                }
                res = runner.run_job(job, target_conn, runner_cancel) => res,
            };

            let success = match outcome {
                Ok(()) => true,
                Err(err) => {
                    tracing::warn!(
                        state_epoch,
                        ?job,
                        error = %err,
                        "background lifecycle job failed"
                    );
                    false
                }
            };

            let _ = tx
                .send(HaEvent::BackgroundJobFinished {
                    state_epoch,
                    now: Instant::now(),
                    job,
                    success,
                })
                .await;
        });

        self.active = Some(ActiveJob {
            state_epoch,
            job,
            cancel_tx,
            abort_handle: join_handle.abort_handle(),
        });
    }

    /// Signals cooperative cancellation (`watch` channel) and aborts the spawned task.
    pub fn cancel_all(&mut self) {
        if let Some(prev) = self.active.take() {
            let _ = prev.cancel_tx.send(true);
            prev.abort_handle.abort();
            tracing::info!(
                state_epoch = prev.state_epoch,
                job = ?prev.job,
                "cancelled active background job"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    struct ControlledRunner {
        hang: bool,
        cancelled_observed: Arc<AtomicBool>,
    }

    impl JobRunner for ControlledRunner {
        async fn run_job(
            &self,
            _job: JobKind,
            _target_conn: Option<String>,
            mut cancel_rx: watch::Receiver<bool>,
        ) -> Result<(), String> {
            if self.hang {
                let _ = cancel_rx.changed().await;
                self.cancelled_observed.store(true, Ordering::SeqCst);
                Err("cancelled".to_owned())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn job_supervisor_reports_completion_and_cancels_cleanly() {
        let (tx, mut rx) = mpsc::channel(8);
        let flag = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(ControlledRunner {
            hang: false,
            cancelled_observed: Arc::clone(&flag),
        });

        let mut sup = JobSupervisor::new(runner, tx.clone());
        sup.spawn(7, JobKind::InitDb, None);

        let ev = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            ev,
            HaEvent::BackgroundJobFinished {
                state_epoch: 7,
                job: JobKind::InitDb,
                success: true,
                ..
            }
        ));

        // Now spawn a hanging job and cancel it: no spurious completion event should arrive.
        let hang_runner = Arc::new(ControlledRunner {
            hang: true,
            cancelled_observed: Arc::clone(&flag),
        });
        let mut hang_sup = JobSupervisor::new(hang_runner, tx);
        hang_sup.spawn(8, JobKind::BaseBackup, Some("postgres://leader".to_owned()));
        assert_eq!(hang_sup.active_job(), Some((8, JobKind::BaseBackup)));
        hang_sup.cancel_all();
        assert!(hang_sup.active_job().is_none());

        let none_ev = tokio::time::timeout(Duration::from_millis(30), rx.recv()).await;
        assert!(none_ev.is_err());
    }
}
