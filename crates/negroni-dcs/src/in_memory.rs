use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use compact_str::CompactString;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState, Versioned,
};
use tokio::sync::{broadcast, mpsc};

use crate::{DcsBackend, DcsError, DcsWatchEvent};

#[derive(Debug, Clone)]
struct LeaseRecord {
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct StoreInner {
    next_rev: u64,
    next_lease: u64,
    leases: BTreeMap<CompactString, LeaseRecord>,
    initialize: Option<Versioned<CompactString>>,
    config: Option<Versioned<serde_json::Value>>,
    leader: Option<(Versioned<CompactString>, CompactString)>,
    members: BTreeMap<CompactString, (Versioned<MemberData>, CompactString)>,
    status: Option<Versioned<ClusterStatus>>,
    sync: Option<Versioned<SyncState>>,
    failover: Option<Versioned<FailoverIntent>>,
    failsafe: Option<BTreeMap<CompactString, String>>,
    history: Vec<HistoryEntry>,
}

impl StoreInner {
    fn bump_rev(&mut self) -> CasToken {
        self.next_rev = self.next_rev.saturating_add(1);
        CasToken::Revision(self.next_rev)
    }

    fn gc_expired_leases(&mut self, now: Instant) -> bool {
        let mut leader_expired = false;
        self.leases.retain(|_, rec| now < rec.expires_at);
        if let Some((_, ref lease_id)) = self.leader
            && !self.leases.contains_key(lease_id)
        {
            self.leader = None;
            leader_expired = true;
        }
        self.members
            .retain(|_, (_, lease_id)| self.leases.contains_key(lease_id));
        leader_expired
    }
}

/// Deterministic thread-safe in-memory [`DcsBackend`] supporting revision CAS, lease TTL
/// expiration, `delete_cluster`, and watch notifications for standalone testing and embedded harnesses.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use negroni_dcs::{DcsBackend, InMemoryDcs};
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let dcs = InMemoryDcs::new();
/// let lease = dcs.ensure_lease(Duration::from_secs(30), None).await?;
/// let tok = dcs.acquire_or_renew_leader("pg-1", &lease, None).await?;
/// let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
/// assert_eq!(snap.leader.as_ref().map(|l| l.value.as_str()), Some("pg-1"));
/// dcs.release_leader("pg-1", &tok).await?;
/// # Ok::<(), negroni_dcs::DcsError>(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct InMemoryDcs {
    inner: Arc<Mutex<StoreInner>>,
    watch_tx: broadcast::Sender<DcsWatchEvent>,
}

impl Default for InMemoryDcs {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InMemoryDcs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryDcs").finish_non_exhaustive()
    }
}

impl InMemoryDcs {
    pub fn new() -> Self {
        let (watch_tx, _) = broadcast::channel(128);
        Self {
            inner: Arc::new(Mutex::new(StoreInner::default())),
            watch_tx,
        }
    }

    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, StoreInner>, DcsError> {
        self.inner
            .lock()
            .map_err(|_| DcsError::Unreachable("in-memory DCS mutex poisoned".to_owned()))
    }

    fn notify(&self, ev: DcsWatchEvent) {
        let _ = self.watch_tx.send(ev);
    }
}

impl DcsBackend for InMemoryDcs {
    async fn fetch_cluster(&self, _timeout: Duration) -> Result<ClusterSnapshot, DcsError> {
        let now = Instant::now();
        let mut g = self.lock_store()?;
        if g.gc_expired_leases(now) {
            self.notify(DcsWatchEvent::LeaderChanged);
        }
        let members = g
            .members
            .iter()
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect();
        Ok(ClusterSnapshot {
            initialize: g.initialize.clone(),
            config: g.config.clone(),
            leader: g.leader.as_ref().map(|(v, _)| v.clone()),
            members,
            status: g.status.clone(),
            sync: g.sync.clone(),
            failover: g.failover.clone(),
            failsafe: g.failsafe.clone(),
            history: g.history.clone(),
        })
    }

    async fn ensure_lease(
        &self,
        ttl: Duration,
        current: Option<&LeaseHandle>,
    ) -> Result<LeaseHandle, DcsError> {
        let now = Instant::now();
        let mut g = self.lock_store()?;
        g.gc_expired_leases(now);
        if let Some(existing) = current
            && let Some(rec) = g.leases.get_mut(&existing.0)
        {
            rec.expires_at = now + ttl;
            return Ok(existing.clone());
        }
        g.next_lease = g.next_lease.saturating_add(1);
        let id = CompactString::from(format!("mem-lease-{}", g.next_lease));
        g.leases.insert(
            id.clone(),
            LeaseRecord {
                expires_at: now + ttl,
            },
        );
        Ok(LeaseHandle(id))
    }

    async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError> {
        let mut g = self.lock_store()?;
        if g.initialize.is_some() {
            return Err(DcsError::CasConflict { expected: None });
        }
        let token = g.bump_rev();
        g.initialize = Some(Versioned {
            value: CompactString::from(sys_id),
            token: token.clone(),
        });
        self.notify(DcsWatchEvent::TopologyChanged);
        Ok(token)
    }

    async fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let now = Instant::now();
        let mut g = self.lock_store()?;
        g.gc_expired_leases(now);
        if !g.leases.contains_key(&lease.0) {
            return Err(DcsError::LeaseExpired);
        }

        match (&g.leader, expected) {
            (None, None) => {
                let token = g.bump_rev();
                g.leader = Some((
                    Versioned {
                        value: CompactString::from(self_name),
                        token: token.clone(),
                    },
                    lease.0.clone(),
                ));
                self.notify(DcsWatchEvent::LeaderChanged);
                Ok(token)
            }
            (Some((cur, _)), Some(exp)) if &cur.token == exp && cur.value == self_name => {
                let token = g.bump_rev();
                g.leader = Some((
                    Versioned {
                        value: CompactString::from(self_name),
                        token: token.clone(),
                    },
                    lease.0.clone(),
                ));
                Ok(token)
            }
            _ => Err(DcsError::CasConflict {
                expected: expected.cloned(),
            }),
        }
    }

    async fn release_leader(&self, self_name: &str, expected: &CasToken) -> Result<(), DcsError> {
        let mut g = self.lock_store()?;
        match &g.leader {
            Some((cur, _)) if &cur.token == expected && cur.value == self_name => {
                g.leader = None;
                self.notify(DcsWatchEvent::LeaderChanged);
                Ok(())
            }
            _ => Err(DcsError::CasConflict {
                expected: Some(expected.clone()),
            }),
        }
    }

    async fn upsert_member(
        &self,
        self_name: &str,
        data: &MemberData,
        lease: &LeaseHandle,
    ) -> Result<(), DcsError> {
        let now = Instant::now();
        let mut g = self.lock_store()?;
        g.gc_expired_leases(now);
        if !g.leases.contains_key(&lease.0) {
            return Err(DcsError::LeaseExpired);
        }
        let token = g.bump_rev();
        g.members.insert(
            CompactString::from(self_name),
            (
                Versioned {
                    value: data.clone(),
                    token,
                },
                lease.0.clone(),
            ),
        );
        self.notify(DcsWatchEvent::TopologyChanged);
        Ok(())
    }

    async fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> Result<(), DcsError> {
        let mut g = self.lock_store()?;
        let token = g.bump_rev();
        g.status = Some(Versioned {
            value: status.clone(),
            token,
        });
        if let Some(roster) = failsafe_roster {
            g.failsafe = Some(roster.clone());
        }
        Ok(())
    }

    async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError> {
        let mut g = self.lock_store()?;
        g.history.push(entry.clone());
        Ok(())
    }

    async fn cas_sync_state(
        &self,
        next: &SyncState,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let mut g = self.lock_store()?;
        let matches = match (&g.sync, expected) {
            (None, None) => true,
            (Some(cur), Some(exp)) => &cur.token == exp,
            _ => false,
        };
        if !matches {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        let token = g.bump_rev();
        g.sync = Some(Versioned {
            value: next.clone(),
            token: token.clone(),
        });
        Ok(token)
    }

    async fn cas_config(
        &self,
        config_json: &serde_json::Value,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let mut g = self.lock_store()?;
        let matches = match (&g.config, expected) {
            (None, None) => true,
            (Some(cur), Some(exp)) => &cur.token == exp,
            _ => false,
        };
        if !matches {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        let token = g.bump_rev();
        g.config = Some(Versioned {
            value: config_json.clone(),
            token: token.clone(),
        });
        self.notify(DcsWatchEvent::ConfigChanged);
        Ok(token)
    }

    async fn cas_failover(
        &self,
        intent: &FailoverIntent,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let mut g = self.lock_store()?;
        let matches = match (&g.failover, expected) {
            (None, None) => true,
            (Some(cur), Some(exp)) => &cur.token == exp,
            _ => false,
        };
        if !matches {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        let token = g.bump_rev();
        g.failover = Some(Versioned {
            value: intent.clone(),
            token: token.clone(),
        });
        self.notify(DcsWatchEvent::FailoverRequested);
        Ok(token)
    }

    async fn clear_failover(&self, expected: &CasToken) -> Result<(), DcsError> {
        let mut g = self.lock_store()?;
        match &g.failover {
            Some(cur) if &cur.token == expected => {
                g.failover = None;
                self.notify(DcsWatchEvent::FailoverRequested);
                Ok(())
            }
            _ => Err(DcsError::CasConflict {
                expected: Some(expected.clone()),
            }),
        }
    }

    async fn delete_cluster(&self) -> Result<(), DcsError> {
        let mut g = self.lock_store()?;
        g.initialize = None;
        g.config = None;
        g.leader = None;
        g.members.clear();
        g.status = None;
        g.sync = None;
        g.failover = None;
        g.failsafe = None;
        g.history.clear();
        self.notify(DcsWatchEvent::LeaderChanged);
        Ok(())
    }

    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent> {
        let (tx, rx) = mpsc::channel(64);
        let mut bcast_rx = self.watch_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match bcast_rx.recv().await {
                    Ok(ev) => {
                        if tx.send(ev).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if tx.send(DcsWatchEvent::TopologyChanged).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        rx
    }
}
