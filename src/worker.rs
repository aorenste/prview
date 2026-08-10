use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use rusqlite::Connection;
use tokio::sync::broadcast;

use crate::db;
use crate::github;

#[derive(Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum PrUpdate {
    #[serde(rename = "changed")]
    Changed(db::PrRow),
    #[serde(rename = "removed")]
    Removed { repo: String, number: i64 },
}

#[derive(Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum ReviewPrUpdate {
    #[serde(rename = "changed")]
    Changed(db::ReviewPrRow),
    #[serde(rename = "removed")]
    Removed { repo: String, number: i64 },
}

#[derive(Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum IssueUpdate {
    #[serde(rename = "changed")]
    Changed(db::IssueRow),
    #[serde(rename = "removed")]
    Removed { repo: String, number: i64 },
}

#[derive(Clone, serde::Serialize)]
pub struct UpdateBatch {
    pub target_user: String,
    pub pr_updates: Vec<PrUpdate>,
    pub review_updates: Vec<ReviewPrUpdate>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub merged_prs: Vec<db::MergedPrRow>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issue_updates: Vec<IssueUpdate>,
    pub hidden_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn fetch_prs_loop(
    db: Arc<Mutex<Connection>>,
    interval: std::time::Duration,
    tx: broadcast::Sender<UpdateBatch>,
    nudge: Arc<AtomicBool>,
    gh_user: Arc<String>,
) {
    loop {
        // A pending nudge means this iteration was triggered by a manual
        // refresh; force a full re-check (e.g. CI-approval) rather than relying
        // on updated_at having changed. Clears the flag for this run.
        let force = nudge.swap(false, Ordering::Relaxed);
        let user = gh_user.as_str();

        // Cycle-start marker + duration: a "starting" with no matching "Fetched"
        // means the loop wedged mid-cycle (e.g. a hung request).
        log!("[{}] Fetch cycle starting{}", user, if force { " (forced)" } else { "" });
        let started = Instant::now();
        match fetch_and_store(&db, &tx, user, force).await {
            Ok((my_count, review_count)) => {
                log!("[{}] Fetched {} open PRs, {} review-requested PRs in {:.1}s",
                    user, my_count, review_count, started.elapsed().as_secs_f64());
            }
            Err(e) => {
                log!("[{}] Error fetching PRs: {}", user, e);
                let _ = tx.send(UpdateBatch {
                    target_user: user.to_string(),
                    pr_updates: vec![],
                    review_updates: vec![],
                    merged_prs: vec![],
                    issue_updates: vec![],
                    hidden_count: 0,
                    error: Some(e.to_string()),
                });
            }
        }

        // If nudged during fetch, skip the sleep and loop immediately
        if nudge.load(Ordering::Relaxed) {
            log!("Refresh requested during fetch, re-fetching immediately");
            continue;
        }

        // Sleep with periodic nudge checks
        let sleep_ms = interval.as_millis() as u64;
        let check_interval = 200u64; // check every 200ms
        let mut elapsed = 0u64;
        while elapsed < sleep_ms {
            tokio::time::sleep(std::time::Duration::from_millis(check_interval)).await;
            elapsed += check_interval;
            if nudge.load(Ordering::Relaxed) {
                log!("Manual refresh requested");
                break;
            }
        }
    }
}

pub async fn fetch_details_loop(
    db: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<UpdateBatch>,
    gh_user: Arc<String>,
) {
    // Consecutive cycles each PR has been re-selected for a detail fetch. Used
    // to warn about a PR that never stops being stale (e.g. a frozen
    // checks_overall='PENDING' the main loop isn't refreshing).
    let mut refetch_streak: HashMap<(String, i64), u32> = HashMap::new();
    loop {
        let user = gh_user.as_str();
        let label = user;
        let mention_user = user;

        {
            // Refresh details for changed PRs, and once when CI settles (see
            // list_stale_prs). Still-running PRs are NOT re-polled.
            const STALE_MAX_AGE: i64 = 60;
            let (stale_prs, stale_reviews) = {
                let conn = db.lock().unwrap();
                (
                    db::list_stale_prs(&conn, user, STALE_MAX_AGE),
                    db::list_stale_review_prs(&conn, user, STALE_MAX_AGE),
                )
            };

            // Group by repo: (number, include_landing) — my PRs get true, reviews get false
            let mut by_repo: HashMap<String, Vec<(i64, bool)>> = HashMap::new();
            // Track which table each PR belongs to for updating DB + SSE
            let mut is_my_pr: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();
            let mut is_review_pr: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();

            // Why each PR was selected (never-fetched / updated / ci-pending),
            // for logging.
            let mut reasons: HashMap<(String, i64), String> = HashMap::new();

            for (repo, number, reason) in &stale_prs {
                by_repo.entry(repo.clone()).or_default().push((*number, true));
                is_my_pr.insert((repo.clone(), *number));
                reasons.insert((repo.clone(), *number), reason.clone());
            }
            for (repo, number, reason) in &stale_reviews {
                let entry = by_repo.entry(repo.clone()).or_default();
                if !entry.iter().any(|(n, _)| *n == *number) {
                    entry.push((*number, false));
                }
                is_review_pr.insert((repo.clone(), *number));
                reasons.entry((repo.clone(), *number)).or_insert_with(|| reason.clone());
            }

            // Loop detector: bump a streak for every PR selected this cycle, reset
            // for those that dropped out, and warn when one keeps getting picked.
            let selected: std::collections::HashSet<(String, i64)> =
                reasons.keys().cloned().collect();
            refetch_streak.retain(|k, _| selected.contains(k));
            for key in &selected {
                let streak = refetch_streak.entry(key.clone()).or_insert(0);
                *streak += 1;
                if *streak == 10 || (*streak > 10 && *streak % 30 == 0) {
                    let reason = reasons.get(key).map(|s| s.as_str()).unwrap_or("?");
                    log!("[{}] WARNING: {}#{} re-fetched {} cycles running (reason: {}) — \
                          main loop may be stale or this PR's detail fetch keeps failing",
                        label, key.0, key.1, *streak, reason);
                }
            }

            for (repo, prs) in &by_repo {
                // Process in chunks of DETAIL_BATCH_SIZE
                for chunk in prs.chunks(github::DETAIL_BATCH_SIZE) {
                    let nums: Vec<String> = chunk.iter().map(|(n, _)| {
                        let reason = reasons.get(&(repo.clone(), *n)).map(|s| s.as_str()).unwrap_or("?");
                        format!("{}({})", n, reason)
                    }).collect();
                    log!("[{}] Detail batch: {} [{}]", label, repo, nums.join(", "));

                    match github::fetch_pr_details_batch(repo, chunk, &mention_user).await {
                        Ok((results, needs_pagination)) => {
                            let mut pr_updates = Vec::new();
                            let mut review_updates = Vec::new();

                            for (number, details) in &results {
                                let conn = db.lock().unwrap();
                                if is_my_pr.contains(&(repo.clone(), *number)) {
                                    db::update_pr_details(&conn, repo, *number, user, details);
                                    if let Some(pr) = db::get_pr(&conn, repo, *number, user) {
                                        pr_updates.push(PrUpdate::Changed(pr));
                                    }
                                }
                                if is_review_pr.contains(&(repo.clone(), *number)) {
                                    db::update_review_pr_details(&conn, repo, *number, user, details);
                                    if let Some(pr) = db::get_review_pr(&conn, repo, *number, user) {
                                        review_updates.push(ReviewPrUpdate::Changed(pr));
                                    }
                                }
                            }

                            if !pr_updates.is_empty() || !review_updates.is_empty() {
                                let hidden_count = {
                                    let conn = db.lock().unwrap();
                                    db::hidden_count(&conn, user)
                                };
                                let _ = tx.send(UpdateBatch {
                                    target_user: user.to_string(),
                                    pr_updates,
                                    review_updates,
                                    merged_prs: vec![],
                                    issue_updates: vec![],
                                    hidden_count,
                                    error: None,
                                });
                            }

                            // Fall back to individual fetches for PRs needing pagination
                            for (number, include_landing) in &needs_pagination {
                                let details = match github::fetch_pr_details(repo, *number, *include_landing, &mention_user).await {
                                    Ok(d) => d,
                                    Err(e) => {
                                        log!("[{}] Detail pagination failed for {}#{}: {} (will retry next cycle)",
                                            label, repo, number, e);
                                        continue;
                                    }
                                };
                                {
                                    let conn = db.lock().unwrap();
                                    if is_my_pr.contains(&(repo.clone(), *number)) {
                                        db::update_pr_details(&conn, repo, *number, user, &details);
                                        if let Some(pr) = db::get_pr(&conn, repo, *number, user) {
                                            let hidden_count = db::hidden_count(&conn, user);
                                            let _ = tx.send(UpdateBatch {
                                                target_user: user.to_string(),
                                                pr_updates: vec![PrUpdate::Changed(pr)],
                                                review_updates: vec![],
                                                merged_prs: vec![],
                                                issue_updates: vec![],
                                                hidden_count,
                                                error: None,
                                            });
                                        }
                                    }
                                    if is_review_pr.contains(&(repo.clone(), *number)) {
                                        db::update_review_pr_details(&conn, repo, *number, user, &details);
                                        if let Some(pr) = db::get_review_pr(&conn, repo, *number, user) {
                                            let hidden_count = db::hidden_count(&conn, user);
                                            let _ = tx.send(UpdateBatch {
                                                target_user: user.to_string(),
                                                pr_updates: vec![],
                                                review_updates: vec![ReviewPrUpdate::Changed(pr)],
                                                merged_prs: vec![],
                                                issue_updates: vec![],
                                                hidden_count,
                                                error: None,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log!("[{}] Detail batch error for {}: {}", label, repo, e);
                        }
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

/// Whether to re-run the (REST) CI-approval check for a review PR.
///
/// Normally we skip PRs whose `updated_at` is unchanged and reuse the stored
/// value, to avoid an API call per PR every poll. The downside is that
/// approving CI workflows doesn't bump `updated_at`, so a cleared CI-approval
/// can stay stale. A manual refresh passes `force` to re-check everything.
fn should_recheck_ci_approval(old_updated_at: Option<&str>, new_updated_at: &str, force: bool) -> bool {
    if force {
        return true;
    }
    match old_updated_at {
        Some(old) => old != new_updated_at, // re-check only if the PR actually changed
        None => true,                       // never seen before — must check
    }
}

async fn fetch_and_store(
    db: &Arc<Mutex<Connection>>,
    tx: &broadcast::Sender<UpdateBatch>,
    user: &str,
    force: bool,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    // Snapshot old state
    let old_prs: HashMap<(String, i64), db::PrRow> = {
        let conn = db.lock().unwrap();
        db::list_prs(&conn, true, user)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let old_reviews: HashMap<(String, i64), db::ReviewPrRow> = {
        let conn = db.lock().unwrap();
        db::list_review_prs(&conn, user)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let old_issues: HashMap<(String, i64), db::IssueRow> = {
        let conn = db.lock().unwrap();
        db::list_issues(&conn, user)
            .into_iter()
            .map(|i| ((i.repo.clone(), i.number), i))
            .collect()
    };

    // Single GraphQL call gets everything
    let mut result = github::fetch_all_prs(user).await?;
    let my_count = result.my_prs.len();
    let review_count = result.review_prs.len();
    let label = if user.is_empty() { "@me" } else { user };

    // Check CI approval for review PRs whose updated_at changed (or all of them
    // on a forced manual refresh). Unchanged PRs reuse the stored value.
    let mut ci_check_indices = Vec::new();
    for (i, pr) in result.review_prs.iter_mut().enumerate() {
        let key = (pr.repo.clone(), pr.number);
        match old_reviews.get(&key) {
            Some(old) if !should_recheck_ci_approval(Some(&old.updated_at), &pr.updated_at, force) => {
                pr.ci_approval_needed = old.ci_approval_needed;
            }
            _ => ci_check_indices.push(i),
        }
    }
    if !ci_check_indices.is_empty() {
        let futures: Vec<_> = ci_check_indices.iter()
            .map(|&i| {
                let repo = result.review_prs[i].repo.clone();
                let sha = result.review_prs[i].head_sha.clone();
                async move { github::check_ci_approval_needed(&repo, &sha).await }
            })
            .collect();
        let results = futures::future::join_all(futures).await;
        for (&idx, needed) in ci_check_indices.iter().zip(results) {
            result.review_prs[idx].ci_approval_needed = needed;
        }
        log!("[{}] Checked CI approval for {} review PRs (of {}){}",
            label, ci_check_indices.len(), review_count,
            if force { " [forced]" } else { "" });
    }

    // Upsert: never delete a row just because it's missing from a search
    // result — absence isn't affirmative. Closures are handled below via
    // affirmative signals (merged_prs and closed_reviewed).
    {
        let conn = db.lock().unwrap();
        db::upsert_prs(&conn, &result.my_prs, user)?;
        db::upsert_review_prs(&conn, &result.review_prs, user)?;
        let _ = db::upsert_issues(&conn, &result.issues, user);

        // Affirmative closure signals: closed_authored covers ALL authored PRs
        // that closed (including manually closed, not just merged). Also
        // closed_reviewed covers PRs the user reviewed but didn't author.
        for (repo, number) in &result.closed_authored {
            db::delete_pr(&conn, repo, *number, user);
            db::delete_review_pr(&conn, repo, *number, user);
        }
        for (repo, number) in &result.closed_reviewed {
            db::delete_review_pr(&conn, repo, *number, user);
        }
    }

    // For review_prs that disappeared from the open-PR search but weren't caught
    // by any closed query, verify individually — directly (not via search) so
    // it's immune to search-index lag. Remove if the PR closed OR you're no
    // longer a requested reviewer and never reviewed it (added-then-removed).
    {
        let fetched_keys: HashSet<(String, i64)> = result.review_prs.iter()
            .map(|pr| (pr.repo.clone(), pr.number))
            .collect();
        let closed_keys: HashSet<(String, i64)> = result.closed_authored.iter()
            .chain(result.closed_reviewed.iter())
            .cloned()
            .collect();
        let old_review_keys: HashSet<(String, i64)> = old_reviews.keys().cloned().collect();
        let to_verify = keys_needing_verification(&old_review_keys, &fetched_keys, &closed_keys);
        for (repo, number) in &to_verify {
            match github::check_review_relevance(repo, *number, user).await {
                Ok(rel) if review_pr_is_stale(&rel.state, rel.still_requested, rel.has_review) => {
                    log!("[{}] Review PR {}/#{} no longer relevant (state={}, requested={}, reviewed={}), removing",
                        label, repo, number, rel.state, rel.still_requested, rel.has_review);
                    let conn = db.lock().unwrap();
                    db::delete_review_pr(&conn, repo, *number, user);
                }
                Ok(_) => {} // still requested or reviewed — keep (search hiccup)
                Err(e) => {
                    log!("[{}] Failed to check review relevance of {}/#{}: {}", label, repo, number, e);
                }
            }
        }
    }

    // Authored PRs get the same fallback. `closed_authored` is built from a
    // `closed:>DATE` search whose index lags GitHub's closed-state facet, so a
    // just-landed PR (e.g. a ghstack PR stuck showing "landing") can be missing
    // from BOTH the open fetch and the closed set. REST-verify and delete those;
    // otherwise an authored PR would linger forever with a stale landing status.
    {
        let fetched_keys: HashSet<(String, i64)> = result.my_prs.iter()
            .map(|pr| (pr.repo.clone(), pr.number))
            .collect();
        let closed_keys: HashSet<(String, i64)> = result.closed_authored.iter().cloned().collect();
        let old_pr_keys: HashSet<(String, i64)> = old_prs.keys().cloned().collect();
        let to_verify = keys_needing_verification(&old_pr_keys, &fetched_keys, &closed_keys);
        for (repo, number) in &to_verify {
            match github::check_pr_state(repo, *number).await {
                Ok(state) if state != "open" => {
                    log!("[{}] Authored PR {}/#{} is {}, removing", label, repo, number, state);
                    let conn = db.lock().unwrap();
                    db::delete_pr(&conn, repo, *number, user);
                }
                Ok(_) => {} // still open — search hiccup, leave it
                Err(e) => {
                    log!("[{}] Failed to check state of PR {}/#{}: {}", label, repo, number, e);
                }
            }
        }
    }

    // Issues have no closed-issue query (unlike PRs). The open-issue fetch
    // returns the complete current set, so any tracked issue missing from a
    // *successful* fetch was closed or unassigned. REST-verify each before
    // deleting so a transient search hiccup can't drop a still-open issue.
    if result.issues_ok {
        let fetched_keys: HashSet<(String, i64)> = result.issues.iter()
            .map(|i| (i.repo.clone(), i.number))
            .collect();
        let old_issue_keys: HashSet<(String, i64)> = old_issues.keys().cloned().collect();
        let to_verify = issues_needing_verification(&old_issue_keys, &fetched_keys);
        for (repo, number) in &to_verify {
            match github::check_issue_state(repo, *number).await {
                Ok(state) if state != "open" => {
                    log!("[{}] Issue {}/#{} is {}, removing", label, repo, number, state);
                    let conn = db.lock().unwrap();
                    db::delete_issue(&conn, repo, *number, user);
                }
                Ok(_) => {} // still open — search hiccup, leave it
                Err(e) => {
                    log!("[{}] Failed to check state of issue {}/#{}: {}", label, repo, number, e);
                }
            }
        }
    }

    // Compute diffs
    let new_prs: HashMap<(String, i64), db::PrRow> = {
        let conn = db.lock().unwrap();
        db::list_prs(&conn, true, user)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let new_reviews: HashMap<(String, i64), db::ReviewPrRow> = {
        let conn = db.lock().unwrap();
        db::list_review_prs(&conn, user)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let new_issues: HashMap<(String, i64), db::IssueRow> = {
        let conn = db.lock().unwrap();
        db::list_issues(&conn, user)
            .into_iter()
            .map(|i| ((i.repo.clone(), i.number), i))
            .collect()
    };

    let mut pr_updates = Vec::new();
    for (key, new_pr) in &new_prs {
        match old_prs.get(key) {
            Some(old_pr) if old_pr == new_pr => {}
            _ => pr_updates.push(PrUpdate::Changed(new_pr.clone())),
        }
    }
    for (key, _) in &old_prs {
        if !new_prs.contains_key(key) {
            pr_updates.push(PrUpdate::Removed {
                repo: key.0.clone(),
                number: key.1,
            });
        }
    }

    let mut review_updates = Vec::new();
    for (key, new_pr) in &new_reviews {
        match old_reviews.get(key) {
            Some(old_pr) if old_pr == new_pr => {}
            _ => review_updates.push(ReviewPrUpdate::Changed(new_pr.clone())),
        }
    }
    for (key, _) in &old_reviews {
        if !new_reviews.contains_key(key) {
            review_updates.push(ReviewPrUpdate::Removed {
                repo: key.0.clone(),
                number: key.1,
            });
        }
    }

    let mut issue_updates = Vec::new();
    for (key, new_issue) in &new_issues {
        match old_issues.get(key) {
            Some(old_issue) if old_issue == new_issue => {}
            _ => issue_updates.push(IssueUpdate::Changed(new_issue.clone())),
        }
    }
    for (key, _) in &old_issues {
        if !new_issues.contains_key(key) {
            issue_updates.push(IssueUpdate::Removed {
                repo: key.0.clone(),
                number: key.1,
            });
        }
    }

    // Store merged PRs and compute diff
    let old_merged: Vec<db::MergedPrRow> = {
        let conn = db.lock().unwrap();
        db::list_merged_prs(&conn, user)
    };
    let skip_merged = !old_merged.is_empty() && result.merged_prs.is_empty();
    if skip_merged {
        log!("[{}] Suspicious empty merged_prs result (had {}), skipping DB replace",
            label, old_merged.len());
    }
    if !skip_merged {
        let conn = db.lock().unwrap();
        let _ = db::replace_merged_prs(&conn, &result.merged_prs, user);
    }
    let new_merged: Vec<db::MergedPrRow> = {
        let conn = db.lock().unwrap();
        db::list_merged_prs(&conn, user)
    };
    let merged_prs = if old_merged != new_merged { new_merged } else { vec![] };

    let hidden_count = {
        let conn = db.lock().unwrap();
        db::hidden_count(&conn, user)
    };
    let _ = tx.send(UpdateBatch {
        target_user: user.to_string(),
        pr_updates,
        review_updates,
        merged_prs,
        issue_updates,
        hidden_count,
        error: None,
    });

    Ok((my_count, review_count))
}

use std::collections::HashSet;

/// Identify tracked PRs (authored or review-requested) that were in the DB but
/// missing from BOTH the open-PR search results AND the closed-query results.
/// GitHub's `closed:>DATE` search index lags the closed-state facet, so a
/// just-closed PR can be absent from both even though it's really closed; these
/// need an individual REST state check to decide whether to keep (search hiccup)
/// or delete (truly closed). Returns (repo, number) pairs.
fn keys_needing_verification(
    old_keys: &HashSet<(String, i64)>,
    fetched_keys: &HashSet<(String, i64)>,
    closed_keys: &HashSet<(String, i64)>,
) -> Vec<(String, i64)> {
    old_keys.iter()
        .filter(|k| !fetched_keys.contains(k) && !closed_keys.contains(k))
        .cloned()
        .collect()
}

/// Decide whether a review PR that dropped out of the search results should be
/// removed, given its authoritative (non-search) review relationship. Remove it
/// if the PR closed, OR it's open but you're neither a current requested reviewer
/// nor have you left a review — i.e. you were un-requested and never reviewed it
/// (a search hiccup, by contrast, still shows you as requested/having reviewed).
fn review_pr_is_stale(state: &str, still_requested: bool, has_review: bool) -> bool {
    !state.eq_ignore_ascii_case("open") || (!still_requested && !has_review)
}

/// Identify issues that were in the DB but missing from a *successful* open-issue
/// fetch. Unlike PRs there's no closed-issue query, so absence from the complete
/// open set is the signal; each candidate is REST-verified before deletion so a
/// transient search hiccup can't drop a still-open issue. Returns (repo, number).
fn issues_needing_verification(
    old_issue_keys: &HashSet<(String, i64)>,
    fetched_keys: &HashSet<(String, i64)>,
) -> Vec<(String, i64)> {
    old_issue_keys.iter()
        .filter(|k| !fetched_keys.contains(k))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(repo: &str, number: i64) -> (String, i64) {
        (repo.to_string(), number)
    }

    #[test]
    fn absent_and_not_in_closed_needs_verification() {
        let old = HashSet::from([key("pytorch/pytorch", 182427)]);
        let fetched = HashSet::new();
        let closed = HashSet::new();
        let missing = keys_needing_verification(&old, &fetched, &closed);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].1, 182427);
    }

    #[test]
    fn absent_but_in_closed_already_handled() {
        let old = HashSet::from([key("pytorch/pytorch", 100)]);
        let fetched = HashSet::new();
        let closed = HashSet::from([key("pytorch/pytorch", 100)]);
        let missing = keys_needing_verification(&old, &fetched, &closed);
        assert!(missing.is_empty());
    }

    #[test]
    fn authored_pr_missing_from_open_and_closed_needs_verification() {
        // Repro for #190160: a just-landed ghstack PR is absent from the open
        // fetch AND from the lagging `closed:>DATE` search, so closed_authored
        // misses it. Without a REST-verify fallback it lingers showing "landing".
        let old = HashSet::from([key("pytorch/pytorch", 190160)]);
        let fetched = HashSet::new(); // not in open authored set (it's closed)
        let closed = HashSet::new();  // closed:>DATE index hasn't caught it yet
        let missing = keys_needing_verification(&old, &fetched, &closed);
        assert_eq!(missing, vec![key("pytorch/pytorch", 190160)]);
    }

    #[test]
    fn present_in_fetch_not_missing() {
        let old = HashSet::from([key("pytorch/pytorch", 200)]);
        let fetched = HashSet::from([key("pytorch/pytorch", 200)]);
        let closed = HashSet::new();
        let missing = keys_needing_verification(&old, &fetched, &closed);
        assert!(missing.is_empty());
    }

    #[test]
    fn review_pr_removed_when_unrequested_and_never_reviewed() {
        // Repro for #192633: you were added as a reviewer then removed, and never
        // reviewed. The PR is still OPEN, so the old state-only check kept it.
        assert!(review_pr_is_stale("open", false, false));
    }

    #[test]
    fn review_pr_kept_when_still_requested_or_reviewed() {
        // Still a requested reviewer -> keep (a search hiccup shouldn't drop it).
        assert!(!review_pr_is_stale("open", true, false));
        // You left a review -> keep (reviewed-by would normally surface it).
        assert!(!review_pr_is_stale("open", false, true));
        assert!(!review_pr_is_stale("OPEN", true, true)); // case-insensitive state
    }

    #[test]
    fn review_pr_removed_when_closed_regardless() {
        // A closed PR leaves the review list no matter the relationship.
        assert!(review_pr_is_stale("closed", true, true));
        assert!(review_pr_is_stale("merged", false, true));
    }

    #[test]
    fn issue_absent_from_open_fetch_needs_verification() {
        // Repro for #137874: an issue we track is no longer in the open-issue
        // fetch (it was closed), so it must be flagged for a state check.
        let old = HashSet::from([key("pytorch/pytorch", 137874)]);
        let fetched = HashSet::new();
        let missing = issues_needing_verification(&old, &fetched);
        assert_eq!(missing, vec![key("pytorch/pytorch", 137874)]);
    }

    #[test]
    fn issue_present_in_open_fetch_not_verified() {
        let old = HashSet::from([key("pytorch/pytorch", 42)]);
        let fetched = HashSet::from([key("pytorch/pytorch", 42)]);
        assert!(issues_needing_verification(&old, &fetched).is_empty());
    }

    #[test]
    fn ci_approval_skipped_when_unchanged_and_not_forced() {
        assert!(!should_recheck_ci_approval(Some("t1"), "t1", false));
    }

    #[test]
    fn ci_approval_rechecked_when_updated_at_changed() {
        assert!(should_recheck_ci_approval(Some("t1"), "t2", false));
    }

    #[test]
    fn ci_approval_rechecked_for_new_pr() {
        assert!(should_recheck_ci_approval(None, "t1", false));
    }

    #[test]
    fn ci_approval_forced_rechecks_even_when_unchanged() {
        // Manual refresh: re-check regardless of updated_at so a cleared
        // CI-approval is noticed promptly.
        assert!(should_recheck_ci_approval(Some("t1"), "t1", true));
    }
}
