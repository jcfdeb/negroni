use std::io::{self, Write as _};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use negroni_dcs::DcsBackend;
use negroni_types::{ClusterSnapshot, FailoverIntent};
use serde::Serialize;

use crate::list::render_cluster_list;
use crate::{CliRestClient, OutputFormat};

#[derive(Debug, Serialize)]
struct SwitchoverPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    leader: Option<CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate: Option<CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduled_at: Option<String>,
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_owned())
}

fn select_target_api_url(
    snap: &ClusterSnapshot,
    preferred: Option<&CompactString>,
) -> Option<String> {
    if let Some(name) = preferred
        && let Some(m) = snap.members.get(name)
    {
        return Some(m.value.api_url.clone());
    }
    if let Some(ref ldr) = snap.leader
        && let Some(m) = snap.members.get(&ldr.value)
    {
        return Some(m.value.api_url.clone());
    }
    snap.members
        .values()
        .next()
        .map(|m| m.value.api_url.clone())
}

pub struct SwitchoverArgs {
    pub scope: String,
    pub leader: Option<String>,
    pub candidate: Option<String>,
    pub scheduled: Option<String>,
    pub force: bool,
}

pub async fn run_switchover<D: DcsBackend>(
    dcs: &D,
    rest: &CliRestClient,
    args: SwitchoverArgs,
) -> Result<()> {
    let snap = dcs
        .fetch_cluster(Duration::from_secs(5))
        .await
        .context("failed to fetch cluster topology from DCS")?;

    let current_leader = snap
        .leader
        .as_ref()
        .map(|l| l.value.clone())
        .ok_or_else(|| anyhow::anyhow!("This cluster has no leader; use 'failover' instead"))?;

    let mut leader_choice = args.leader.map(CompactString::from);
    let mut cand_choice = args.candidate.map(CompactString::from);
    let mut sched_choice = args.scheduled;

    let candidates: Vec<CompactString> = snap
        .members
        .keys()
        .filter(|m| **m != current_leader)
        .cloned()
        .collect();
    if candidates.is_empty() {
        bail!("No candidates found to switchover to");
    }

    if !args.force {
        println!(
            "Current cluster topology\n{}",
            render_cluster_list(&args.scope, &snap, OutputFormat::Plain, false, false)
        );
        if leader_choice.is_none() {
            let ans = prompt_line(&format!("Primary [{current_leader}]: "))?;
            leader_choice = Some(if ans.is_empty() {
                current_leader.clone()
            } else {
                CompactString::from(ans)
            });
        }
        if cand_choice.is_none() {
            let list_str: Vec<&str> = candidates.iter().map(CompactString::as_str).collect();
            let ans = prompt_line(&format!("Candidate {} []: ", format_args!("{list_str:?}")))?;
            if !ans.is_empty() {
                cand_choice = Some(CompactString::from(ans));
            }
        }
        if sched_choice.is_none() {
            let ans = prompt_line(
                "When should the switchover take place (e.g. 2026-11-12T22:00:00+00:00) [now]: ",
            )?;
            if !ans.is_empty() && !ans.eq_ignore_ascii_case("now") {
                sched_choice = Some(ans);
            }
        }
        let confirm = prompt_line(&format!(
            "Are you sure you want to switchover cluster {}, demoting current leader {}? [y/N]: ",
            args.scope,
            leader_choice.as_deref().unwrap_or(current_leader.as_str())
        ))?;
        if !matches!(confirm.to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Aborting switchover");
            return Ok(());
        }
    } else if leader_choice.is_none() {
        leader_choice = Some(current_leader.clone());
    }

    if sched_choice.as_deref() == Some("now") {
        sched_choice = None;
    }

    let api_base = select_target_api_url(&snap, Some(&current_leader)).ok_or_else(|| {
        anyhow::anyhow!("unable to resolve REST API URL for leader '{current_leader}'")
    })?;
    let payload = SwitchoverPayload {
        leader: leader_choice,
        candidate: cand_choice,
        scheduled_at: sched_choice,
    };

    let msg = rest.post_json(&api_base, "/switchover", &payload).await?;
    println!("{msg}");
    Ok(())
}

pub struct FailoverArgs {
    pub scope: String,
    pub leader: Option<String>,
    pub candidate: Option<String>,
    pub force: bool,
}

pub async fn run_failover<D: DcsBackend>(
    dcs: &D,
    rest: &CliRestClient,
    args: FailoverArgs,
) -> Result<()> {
    let snap = dcs
        .fetch_cluster(Duration::from_secs(5))
        .await
        .context("failed to fetch cluster topology from DCS")?;

    let current_leader = snap.leader.as_ref().map(|l| l.value.clone());
    let mut cand_choice = args.candidate.map(CompactString::from);
    let leader_choice = args
        .leader
        .map(CompactString::from)
        .or_else(|| current_leader.clone());

    let candidates: Vec<CompactString> = snap
        .members
        .keys()
        .filter(|m| Some(*m) != current_leader.as_ref())
        .cloned()
        .collect();
    if candidates.is_empty() {
        bail!("No candidates available to failover to");
    }

    if !args.force {
        println!(
            "Current cluster topology\n{}",
            render_cluster_list(&args.scope, &snap, OutputFormat::Plain, false, false)
        );
        if cand_choice.is_none() {
            let list_str: Vec<&str> = candidates.iter().map(CompactString::as_str).collect();
            let ans = prompt_line(&format!("Candidate {} []: ", format_args!("{list_str:?}")))?;
            if !ans.is_empty() {
                cand_choice = Some(CompactString::from(ans));
            }
        }
        let confirm = prompt_line(&format!(
            "Are you sure you want to failover cluster {}? [y/N]: ",
            args.scope
        ))?;
        if !matches!(confirm.to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Aborting failover");
            return Ok(());
        }
    } else if cand_choice.is_none() {
        cand_choice = candidates.first().cloned();
    }

    if cand_choice.is_none() && leader_choice.is_none() {
        bail!("Failover requires at least --candidate or an existing leader");
    }

    let payload = SwitchoverPayload {
        leader: leader_choice.clone(),
        candidate: cand_choice.clone(),
        scheduled_at: None,
    };

    if let Some(api_base) = select_target_api_url(&snap, cand_choice.as_ref()) {
        match rest.post_json(&api_base, "/failover", &payload).await {
            Ok(msg) => {
                println!("{msg}");
                return Ok(());
            }
            Err(err) => {
                eprintln!(
                    "REST /failover call failed ({err}); falling back to direct DCS /failover intent"
                );
            }
        }
    }

    let intent = FailoverIntent {
        leader: leader_choice,
        candidate: cand_choice,
        scheduled_at: None,
    };
    let expected = snap.failover.as_ref().map(|v| v.token.clone());
    dcs.cas_failover(&intent, expected.as_ref())
        .await
        .context("failed to write /failover intent to DCS")?;
    println!("Failover intent written to DCS");
    Ok(())
}
