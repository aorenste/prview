use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

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
        nudge.store(false, Ordering::Relaxed);
        let user = gh_user.as_str();

        match fetch_and_store(&db, &tx, user).await {
            Ok((my_count, review_count)) => {
                log!("[{}] Fetched {} open PRs, {} review-requested PRs",
                    user, my_count, review_count);
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
    loop {
        let user = gh_user.as_str();
        let label = user;
        let mention_user = user;

        {
            // Collect PRs whose details haven't been fetched in the last 60s
            let (stale_prs, stale_reviews) = {
                let conn = db.lock().unwrap();
                (
                    db::list_stale_prs(&conn, user, 60),
                    db::list_stale_review_prs(&conn, user, 60),
                )
            };

            // Group by repo: (number, include_landing) — my PRs get true, reviews get false
            let mut by_repo: HashMap<String, Vec<(i64, bool)>> = HashMap::new();
            // Track which table each PR belongs to for updating DB + SSE
            let mut is_my_pr: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();
            let mut is_review_pr: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();

            for (repo, number) in &stale_prs {
                by_repo.entry(repo.clone()).or_default().push((*number, true));
                is_my_pr.insert((repo.clone(), *number));
            }
            for (repo, number) in &stale_reviews {
                let entry = by_repo.entry(repo.clone()).or_default();
                if !entry.iter().any(|(n, _)| *n == *number) {
                    entry.push((*number, false));
                }
                is_review_pr.insert((repo.clone(), *number));
            }

            for (repo, prs) in &by_repo {
                // Process in chunks of DETAIL_BATCH_SIZE
                for chunk in prs.chunks(github::DETAIL_BATCH_SIZE) {
                    let nums: Vec<i64> = chunk.iter().map(|(n, _)| *n).collect();
                    log!("[{}] Detail batch: {} {:?}", label, repo, nums);

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
                                if let Ok(details) = github::fetch_pr_details(repo, *number, *include_landing, &mention_user).await {
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

async fn fetch_and_store(
    db: &Arc<Mutex<Connection>>,
    tx: &broadcast::Sender<UpdateBatch>,
    user: &str,
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

    // Check CI approval only for review PRs whose updated_at changed
    let mut ci_check_indices = Vec::new();
    for (i, pr) in result.review_prs.iter_mut().enumerate() {
        let key = (pr.repo.clone(), pr.number);
        if let Some(old) = old_reviews.get(&key) {
            if old.updated_at == pr.updated_at {
                // Unchanged — preserve old ci_approval_needed value
                pr.ci_approval_needed = old.ci_approval_needed;
                continue;
            }
        }
        ci_check_indices.push(i);
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
        log!("[{}] Checked CI approval for {} changed review PRs (of {})",
            label, ci_check_indices.len(), review_count);
    }

    // Upsert: never delete a row just because it's missing from a search
    // result — absence isn't affirmative. Closures are handled below via
    // affirmative signals (merged_prs and closed_reviewed).
    {
        let conn = db.lock().unwrap();
        db::upsert_prs(&conn, &result.my_prs, user)?;
        db::upsert_review_prs(&conn, &result.review_prs, user)?;
        let _ = db::upsert_issues(&conn, &result.issues, user);

        // Affirmative closure signals: merged_prs (closed PRs the user
        // authored) and closed_reviewed (closed PRs the user reviewed but
        // didn't author). Either is grounds for removing from the open tables.
        for pr in &result.merged_prs {
            db::delete_pr(&conn, &pr.repo, pr.number, user);
            db::delete_review_pr(&conn, &pr.repo, pr.number, user);
        }
        for (repo, number) in &result.closed_reviewed {
            db::delete_review_pr(&conn, repo, *number, user);
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
