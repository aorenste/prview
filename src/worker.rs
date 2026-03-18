use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
pub struct UpdateBatch {
    pub pr_updates: Vec<PrUpdate>,
    pub review_updates: Vec<ReviewPrUpdate>,
    pub hidden_count: i64,
}

pub async fn fetch_prs_loop(
    db: Arc<Mutex<Connection>>,
    interval: std::time::Duration,
    tx: broadcast::Sender<UpdateBatch>,
    nudge: Arc<tokio::sync::Notify>,
) {
    loop {
        match fetch_and_store(&db, &tx).await {
            Ok((my_count, review_count)) => {
                eprintln!("Fetched {} open PRs, {} review-requested PRs", my_count, review_count)
            }
            Err(e) => eprintln!("Error fetching PRs: {}", e),
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = nudge.notified() => {
                eprintln!("Manual refresh requested");
            }
        }
    }
}

async fn fetch_and_store(
    db: &Arc<Mutex<Connection>>,
    tx: &broadcast::Sender<UpdateBatch>,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    // Snapshot old state
    let old_prs: HashMap<(String, i64), db::PrRow> = {
        let conn = db.lock().unwrap();
        db::list_prs(&conn, true)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let old_reviews: HashMap<(String, i64), db::ReviewPrRow> = {
        let conn = db.lock().unwrap();
        db::list_review_prs(&conn)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };

    // Single GraphQL call gets everything
    let result = github::fetch_all_prs().await?;
    let my_count = result.my_prs.len();
    let review_count = result.review_prs.len();

    {
        let conn = db.lock().unwrap();
        db::replace_prs(&conn, &result.my_prs)?;
        db::replace_review_prs(&conn, &result.review_prs)?;
    }

    // Compute diffs
    let new_prs: HashMap<(String, i64), db::PrRow> = {
        let conn = db.lock().unwrap();
        db::list_prs(&conn, true)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
            .collect()
    };
    let new_reviews: HashMap<(String, i64), db::ReviewPrRow> = {
        let conn = db.lock().unwrap();
        db::list_review_prs(&conn)
            .into_iter()
            .map(|pr| ((pr.repo.clone(), pr.number), pr))
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

    if !pr_updates.is_empty() || !review_updates.is_empty() {
        let hidden_count = {
            let conn = db.lock().unwrap();
            db::hidden_count(&conn)
        };
        let _ = tx.send(UpdateBatch { pr_updates, review_updates, hidden_count });
    }

    Ok((my_count, review_count))
}
