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
    active_users: Arc<Mutex<HashMap<String, usize>>>,
) {
    loop {
        nudge.store(false, Ordering::Relaxed);

        let users: Vec<(String, usize)> = {
            let map = active_users.lock().unwrap();
            if map.is_empty() {
                vec![(String::new(), 0)] // default to @me
            } else {
                map.iter().map(|(k, v)| (k.clone(), *v)).collect()
            }
        };

        for (user, _) in &users {
            let label = if user.is_empty() { "@me" } else { user.as_str() };
            match fetch_and_store(&db, &tx, user).await {
                Ok((my_count, review_count)) => {
                    let conns = active_users.lock().unwrap().get(user).copied().unwrap_or(0);
                    eprintln!("[{}] Fetched {} open PRs, {} review-requested PRs ({} conn{})",
                        label, my_count, review_count, conns, if conns == 1 { "" } else { "s" });
                }
                Err(e) => {
                    eprintln!("[{}] Error fetching PRs: {}",
                        label, e);
                    let _ = tx.send(UpdateBatch {
                        target_user: user.clone(),
                        pr_updates: vec![],
                        review_updates: vec![],
                        merged_prs: vec![],
                        issue_updates: vec![],
                        hidden_count: 0,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        // If nudged during fetch, skip the sleep and loop immediately
        if nudge.load(Ordering::Relaxed) {
            eprintln!("Refresh requested during fetch, re-fetching immediately");
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
                eprintln!("Manual refresh requested");
                break;
            }
        }
    }
}

pub async fn fetch_details_loop(
    db: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<UpdateBatch>,
    active_users: Arc<Mutex<HashMap<String, usize>>>,
) {
    loop {
        let users: Vec<String> = {
            let map = active_users.lock().unwrap();
            if map.is_empty() {
                vec![String::new()]
            } else {
                map.keys().cloned().collect()
            }
        };

        for user in &users {
            let label = if user.is_empty() { "@me" } else { user.as_str() };

            // Collect PRs whose details haven't been fetched in the last 60s
            let (stale_prs, stale_reviews) = {
                let conn = db.lock().unwrap();
                (
                    db::list_stale_prs(&conn, user, 60),
                    db::list_stale_review_prs(&conn, user, 60),
                )
            };

            // Process my PRs
            for (repo, number) in &stale_prs {
                match github::fetch_pr_details(repo, *number, true).await {
                    Ok(details) => {
                        let pr = {
                            let conn = db.lock().unwrap();
                            db::update_pr_details(&conn, repo, *number, user, &details);
                            db::get_pr(&conn, repo, *number, user)
                        };
                        if let Some(pr) = pr {
                            eprintln!("[{}] Detail: {}#{} ({} pass, {} fail, {} pending{})",
                                label, repo, number,
                                details.checks_success, details.checks_fail, details.checks_pending,
                                if details.landing_status.is_empty() { String::new() }
                                else { format!(", land: {}", details.landing_status) });
                            let hidden_count = {
                                let conn = db.lock().unwrap();
                                db::hidden_count(&conn, user)
                            };
                            let _ = tx.send(UpdateBatch {
                                target_user: user.clone(),
                                pr_updates: vec![PrUpdate::Changed(pr)],
                                review_updates: vec![],
                                merged_prs: vec![],
                                issue_updates: vec![],
                                hidden_count,
                                error: None,
                            });
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        eprintln!("[{}] Detail error: {}#{}: {}", label, repo, number, e);
                    }
                }
            }

            // Process review PRs
            for (repo, number) in &stale_reviews {
                match github::fetch_pr_details(repo, *number, false).await {
                    Ok(details) => {
                        let review_pr = {
                            let conn = db.lock().unwrap();
                            db::update_review_pr_details(&conn, repo, *number, user, &details);
                            db::get_review_pr(&conn, repo, *number, user)
                        };
                        if let Some(pr) = review_pr {
                            eprintln!("[{}] Detail: {}#{} review ({} pass, {} fail, {} pending)",
                                label, repo, number,
                                details.checks_success, details.checks_fail, details.checks_pending);
                            let hidden_count = {
                                let conn = db.lock().unwrap();
                                db::hidden_count(&conn, user)
                            };
                            let _ = tx.send(UpdateBatch {
                                target_user: user.clone(),
                                pr_updates: vec![],
                                review_updates: vec![ReviewPrUpdate::Changed(pr)],
                                merged_prs: vec![],
                                issue_updates: vec![],
                                hidden_count,
                                error: None,
                            });
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        eprintln!("[{}] Detail error: {}#{} review: {}", label, repo, number, e);
                    }
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
    let result = github::fetch_all_prs(user).await?;
    let my_count = result.my_prs.len();
    let review_count = result.review_prs.len();

    {
        let conn = db.lock().unwrap();
        db::replace_prs(&conn, &result.my_prs, user)?;
        db::replace_review_prs(&conn, &result.review_prs, user)?;
        let _ = db::replace_issues(&conn, &result.issues, user);
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
    {
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
