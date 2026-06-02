use std::path::Path;

use rusqlite::Connection;
use serde::Serialize;

#[derive(Clone, Serialize, PartialEq)]
pub struct PrRow {
    pub repo: String,
    pub number: i64,
    pub title: String,
    pub url: String,
    pub updated_at: String,
    pub hidden: bool,
    pub is_draft: bool,
    pub head_ref_name: String,
    pub base_ref_name: String,
    pub review_status: String,
    pub reviewers: String,
    pub checks_overall: String,
    pub checks_running: bool,
    pub checks_success: i64,
    pub checks_fail: i64,
    pub checks_pending: i64,
    pub checks_queued: i64,
    pub drci_status: String,
    pub drci_emoji: String,
    pub comment_count: i64,
    pub landing_status: String,
    pub head_sha: String,
    pub base_sha: String,
}

#[derive(Clone, Serialize, PartialEq)]
pub struct ReviewPrRow {
    pub repo: String,
    pub number: i64,
    pub title: String,
    pub url: String,
    pub author: String,
    pub is_draft: bool,
    pub head_ref_name: String,
    pub base_ref_name: String,
    pub is_read: bool,
    pub review_status: String,
    pub reviewers: String,
    pub checks_overall: String,
    pub checks_running: bool,
    pub checks_success: i64,
    pub checks_fail: i64,
    pub checks_pending: i64,
    pub checks_queued: i64,
    pub drci_status: String,
    pub drci_emoji: String,
    pub comment_count: i64,
    pub head_sha: String,
    pub updated_at: String,
    pub ci_approval_needed: bool,
    pub base_sha: String,
    pub is_mentioned: bool,
    pub re_review_requested: bool,
    /// When DrCI last refreshed its comment (ISO 8601). Empty if unknown.
    /// Used by the "Only show passing" filter to tell "DrCI is still catching
    /// up" from "DrCI is stuck".
    pub drci_updated_at: String,
}

pub struct PrInsert {
    pub number: i64,
    pub repo: String,
    pub title: String,
    pub url: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub author: String,
    pub is_draft: bool,
    pub head_ref_name: String,
    pub base_ref_name: String,
    pub review_status: String,
    pub reviewers: String,
    pub checks_overall: String,
    pub checks_running: bool,
    pub drci_status: String,
    pub drci_emoji: String,
    pub comment_count: i64,
    pub head_sha: String,
    pub ci_approval_needed: bool,
    pub base_sha: String,
    pub re_review_requested: bool,
}

#[derive(Clone, Serialize, PartialEq)]
pub struct MergedPrRow {
    pub repo: String,
    pub number: i64,
    pub title: String,
    pub url: String,
    pub landed_at: String,
}

#[derive(Clone, Serialize, PartialEq)]
pub struct IssueRow {
    pub repo: String,
    pub number: i64,
    pub title: String,
    pub url: String,
    pub author: String,
    pub created_at: String,
    pub updated_at: String,
    pub comment_count: i64,
    pub labels: String,
}

pub struct IssueInsert {
    pub number: i64,
    pub repo: String,
    pub title: String,
    pub url: String,
    pub author: String,
    pub created_at: String,
    pub updated_at: String,
    pub comment_count: i64,
    pub labels: String,
}

/// Derived from MIGRATIONS so adding a migration can't be forgotten here.
/// Each MIGRATIONS entry takes version v -> v+1, so the latest version equals
/// the number of entries.
const CURRENT_VERSION: i64 = MIGRATIONS.len() as i64;

/// Each entry migrates from version (index) to version (index + 1).
const MIGRATIONS: &[&str] = &[
    // 0 -> 1: initial prs table with all columns
    "CREATE TABLE IF NOT EXISTS prs (
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        hidden INTEGER NOT NULL DEFAULT 0,
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_success INTEGER NOT NULL DEFAULT 0,
        checks_fail INTEGER NOT NULL DEFAULT 0,
        checks_pending INTEGER NOT NULL DEFAULT 0,
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (repo, number)
    )",
    // 1 -> 2: review_prs table
    "CREATE TABLE IF NOT EXISTS review_prs (
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        author TEXT NOT NULL DEFAULT '',
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_success INTEGER NOT NULL DEFAULT 0,
        checks_fail INTEGER NOT NULL DEFAULT 0,
        checks_pending INTEGER NOT NULL DEFAULT 0,
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (repo, number)
    )",
    // 2 -> 3: add checks_overall to review_prs
    "ALTER TABLE review_prs ADD COLUMN checks_overall TEXT NOT NULL DEFAULT ''",
    // 3 -> 4: read/unread state and head_sha for review_prs
    "ALTER TABLE review_prs ADD COLUMN head_sha TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN read_comment_count INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN read_review_status TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN read_head_sha TEXT NOT NULL DEFAULT ''",
    // 4 -> 5: track title changes for auto-unread, CI approval needed
    "ALTER TABLE review_prs ADD COLUMN read_title TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN ci_approval_needed INTEGER NOT NULL DEFAULT 0",
    // 5 -> 6: add target_user to both tables (multi-user support)
    "DROP TABLE IF EXISTS prs;
     DROP TABLE IF EXISTS review_prs;
     CREATE TABLE prs (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        hidden INTEGER NOT NULL DEFAULT 0,
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_success INTEGER NOT NULL DEFAULT 0,
        checks_fail INTEGER NOT NULL DEFAULT 0,
        checks_pending INTEGER NOT NULL DEFAULT 0,
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (target_user, repo, number)
     );
     CREATE TABLE review_prs (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        author TEXT NOT NULL DEFAULT '',
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_overall TEXT NOT NULL DEFAULT '',
        checks_success INTEGER NOT NULL DEFAULT 0,
        checks_fail INTEGER NOT NULL DEFAULT 0,
        checks_pending INTEGER NOT NULL DEFAULT 0,
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        head_sha TEXT NOT NULL DEFAULT '',
        is_read INTEGER NOT NULL DEFAULT 0,
        read_comment_count INTEGER NOT NULL DEFAULT 0,
        read_review_status TEXT NOT NULL DEFAULT '',
        read_head_sha TEXT NOT NULL DEFAULT '',
        read_title TEXT NOT NULL DEFAULT '',
        ci_approval_needed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (target_user, repo, number)
     )",
    // 6 -> 7: add is_draft to prs table
    "ALTER TABLE prs ADD COLUMN is_draft INTEGER NOT NULL DEFAULT 0",
    // 7 -> 8: replace per-check counts with checks_overall on both tables
    "DROP TABLE IF EXISTS prs;
     DROP TABLE IF EXISTS review_prs;
     CREATE TABLE prs (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        hidden INTEGER NOT NULL DEFAULT 0,
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_overall TEXT NOT NULL DEFAULT '',
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (target_user, repo, number)
     );
     CREATE TABLE review_prs (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        author TEXT NOT NULL DEFAULT '',
        is_draft INTEGER NOT NULL DEFAULT 0,
        review_status TEXT NOT NULL DEFAULT '',
        reviewers TEXT NOT NULL DEFAULT '',
        checks_overall TEXT NOT NULL DEFAULT '',
        drci_status TEXT NOT NULL DEFAULT '',
        drci_emoji TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        head_sha TEXT NOT NULL DEFAULT '',
        is_read INTEGER NOT NULL DEFAULT 0,
        read_comment_count INTEGER NOT NULL DEFAULT 0,
        read_review_status TEXT NOT NULL DEFAULT '',
        read_head_sha TEXT NOT NULL DEFAULT '',
        read_title TEXT NOT NULL DEFAULT '',
        ci_approval_needed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (target_user, repo, number)
     )",
    // 8 -> 9: add checks_running to both tables
    "ALTER TABLE prs ADD COLUMN checks_running INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN checks_running INTEGER NOT NULL DEFAULT 0",
    // 9 -> 10: add detail columns for background detail fetcher
    "ALTER TABLE prs ADD COLUMN checks_success INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE prs ADD COLUMN checks_fail INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE prs ADD COLUMN checks_pending INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE prs ADD COLUMN landing_status TEXT NOT NULL DEFAULT '';
     ALTER TABLE prs ADD COLUMN detail_updated_at TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN checks_success INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN checks_fail INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN checks_pending INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN detail_updated_at TEXT NOT NULL DEFAULT ''",
    // 10 -> 11: merged_prs table for recently landed PRs
    "CREATE TABLE merged_prs (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        landed_at TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (target_user, repo, number)
     )",
    // 11 -> 12: issues table for assigned issues
    "CREATE TABLE issues (
        target_user TEXT NOT NULL DEFAULT '',
        number INTEGER NOT NULL,
        repo TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        author TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        comment_count INTEGER NOT NULL DEFAULT 0,
        labels TEXT NOT NULL DEFAULT '[]',
        PRIMARY KEY (target_user, repo, number)
     )",
    // 12 -> 13: add head_ref_name for ghstack grouping
    "ALTER TABLE prs ADD COLUMN head_ref_name TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN head_ref_name TEXT NOT NULL DEFAULT ''",
    // 13 -> 14: add base_ref_name for ghstack chain-based grouping
    "ALTER TABLE prs ADD COLUMN base_ref_name TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN base_ref_name TEXT NOT NULL DEFAULT ''",
    // 14 -> 15: track which updated_at the details were fetched for
    "ALTER TABLE prs ADD COLUMN detail_for_updated_at TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN detail_for_updated_at TEXT NOT NULL DEFAULT ''",
    // 15 -> 16: track updated_at and drci_emoji at mark-read time for smarter auto-unread
    "ALTER TABLE review_prs ADD COLUMN read_updated_at TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN read_drci_emoji TEXT NOT NULL DEFAULT ''",
    // 16 -> 17: base_sha for verifying ghstack chain links
    "ALTER TABLE prs ADD COLUMN base_sha TEXT NOT NULL DEFAULT '';
     ALTER TABLE review_prs ADD COLUMN base_sha TEXT NOT NULL DEFAULT ''",
    // 17 -> 18: head_sha for prs table (was already on review_prs)
    "ALTER TABLE prs ADD COLUMN head_sha TEXT NOT NULL DEFAULT ''",
    // 18 -> 19: mention tracking on review_prs
    "ALTER TABLE review_prs ADD COLUMN mention_count INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN last_mention_count INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN is_mentioned INTEGER NOT NULL DEFAULT 0",
    // 19 -> 20: track queued check count (subset of checks_pending)
    "ALTER TABLE prs ADD COLUMN checks_queued INTEGER NOT NULL DEFAULT 0;
     ALTER TABLE review_prs ADD COLUMN checks_queued INTEGER NOT NULL DEFAULT 0",
    // 20 -> 21: track whether you've been (re-)requested as a reviewer. A pending
    // review request means the ball is in your court even if your last submitted
    // review still makes GitHub's reviewDecision CHANGES_REQUESTED.
    "ALTER TABLE review_prs ADD COLUMN re_review_requested INTEGER NOT NULL DEFAULT 0",
    // 21 -> 22: track when DrCI last refreshed its comment, so the "Only show
    // passing" filter can distinguish DrCI still catching up from DrCI stuck.
    "ALTER TABLE review_prs ADD COLUMN drci_updated_at TEXT NOT NULL DEFAULT ''",
    // 22 -> 23: is_mentioned is now a manual-override-only bit; the effective
    // "mentioned" state is derived as (is_mentioned OR mention_count >
    // last_mention_count). Clear the column once so stale auto-set bits (which
    // used to stick on after a mention was edited/deleted) don't linger. Any
    // genuinely-pending mention still shows via the derived expression.
    "UPDATE review_prs SET is_mentioned = 0",
];

pub fn init_db(path: &Path) -> Connection {
    let conn = Connection::open(path)
        .unwrap_or_else(|e| panic!("Failed to open database {:?}: {}", path, e));

    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap_or(0);

    if version == 0 {
        // Check if this is a pre-versioning DB (has prs table already)
        let has_prs: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='prs'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if has_prs {
            // Legacy DB: prs table exists but no version. The table already has all
            // columns (added via old ALTER TABLE migrations). Just stamp it at version 1
            // and run remaining migrations.
            log!("Migrating legacy database to versioned schema");
            conn.execute_batch("PRAGMA user_version = 1")
                .expect("Failed to set user_version");
            run_migrations(&conn, 1);
            return conn;
        }
    }

    run_migrations(&conn, version);
    conn
}

fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
    let sql = format!("PRAGMA table_info({})", table);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    names.iter().any(|n| n == column)
}

fn run_migrations(conn: &Connection, from_version: i64) {
    for v in from_version..CURRENT_VERSION {
        let idx = v as usize;
        log!("Running migration {} -> {}", v, v + 1);

        // Migration 6->7 adds is_draft, but some v6 DBs already have it
        if v == 6 && has_column(conn, "prs", "is_draft") {
            log!("  is_draft column already exists, skipping ALTER");
        } else {
            conn.execute_batch(MIGRATIONS[idx])
                .unwrap_or_else(|e| panic!("Migration {} -> {} failed: {}", v, v + 1, e));
        }

        conn.execute_batch(&format!("PRAGMA user_version = {}", v + 1))
            .expect("Failed to set user_version");
    }
}

pub fn upsert_prs(conn: &Connection, prs: &[PrInsert], user: &str) -> Result<(), rusqlite::Error> {
    // Upsert: never delete on absence. A PR missing from the search result is
    // never affirmative — could be a GitHub index hiccup or a real closure.
    // Closures are handled separately via merged_prs / direct state checks.
    let mut stmt = conn.prepare(
        "INSERT INTO prs (target_user, number, repo, title, url, state, created_at, updated_at,
                          is_draft, head_ref_name, base_ref_name, head_sha, base_sha,
                          review_status, reviewers, checks_overall, checks_running,
                          drci_status, drci_emoji, comment_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
         ON CONFLICT(target_user, repo, number) DO UPDATE SET
           title = excluded.title,
           url = excluded.url,
           state = excluded.state,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at,
           is_draft = excluded.is_draft,
           head_ref_name = excluded.head_ref_name,
           base_ref_name = excluded.base_ref_name,
           head_sha = excluded.head_sha,
           base_sha = excluded.base_sha,
           review_status = excluded.review_status,
           reviewers = excluded.reviewers,
           checks_overall = excluded.checks_overall,
           checks_running = excluded.checks_running,
           drci_status = excluded.drci_status,
           drci_emoji = excluded.drci_emoji,
           comment_count = excluded.comment_count",
    )?;
    for pr in prs {
        stmt.execute(rusqlite::params![
            user,
            pr.number, pr.repo, pr.title, pr.url, pr.state, pr.created_at, pr.updated_at,
            pr.is_draft as i64, pr.head_ref_name, pr.base_ref_name, pr.head_sha, pr.base_sha,
            pr.review_status, pr.reviewers, pr.checks_overall, pr.checks_running as i64,
            pr.drci_status, pr.drci_emoji, pr.comment_count,
        ])?;
    }
    Ok(())
}

pub fn delete_pr(conn: &Connection, repo: &str, number: i64, user: &str) {
    conn.execute(
        "DELETE FROM prs WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
        rusqlite::params![user, repo, number],
    ).ok();
}

pub fn list_prs(conn: &Connection, show_hidden: bool, user: &str) -> Vec<PrRow> {
    let sql = if show_hidden {
        "SELECT repo, number, title, url, updated_at, hidden, is_draft, head_ref_name, base_ref_name,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count,
                checks_success, checks_fail, checks_pending, landing_status, head_sha, base_sha,
                checks_queued
         FROM prs WHERE target_user = ?1 ORDER BY updated_at DESC"
    } else {
        "SELECT repo, number, title, url, updated_at, hidden, is_draft, head_ref_name, base_ref_name,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count,
                checks_success, checks_fail, checks_pending, landing_status, head_sha, base_sha,
                checks_queued
         FROM prs WHERE target_user = ?1 AND hidden = 0 ORDER BY updated_at DESC"
    };
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map(rusqlite::params![user], |row| {
        Ok(PrRow {
            repo: row.get(0)?,
            number: row.get(1)?,
            title: row.get(2)?,
            url: row.get(3)?,
            updated_at: row.get(4)?,
            hidden: row.get::<_, i64>(5)? != 0,
            is_draft: row.get::<_, i64>(6)? != 0,
            head_ref_name: row.get(7)?,
            base_ref_name: row.get(8)?,
            review_status: row.get(9)?,
            reviewers: row.get(10)?,
            checks_overall: row.get(11)?,
            checks_running: row.get::<_, i64>(12)? != 0,
            drci_status: row.get(13)?,
            drci_emoji: row.get(14)?,
            comment_count: row.get(15)?,
            checks_success: row.get(16)?,
            checks_fail: row.get(17)?,
            checks_pending: row.get(18)?,
            landing_status: row.get(19)?,
            head_sha: row.get(20)?,
            base_sha: row.get(21)?,
            checks_queued: row.get(22)?,
        })
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

pub fn get_pr(conn: &Connection, repo: &str, number: i64, user: &str) -> Option<PrRow> {
    conn.query_row(
        "SELECT repo, number, title, url, updated_at, hidden, is_draft, head_ref_name, base_ref_name,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count,
                checks_success, checks_fail, checks_pending, landing_status, head_sha, base_sha,
                checks_queued
         FROM prs WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
        rusqlite::params![user, repo, number],
        |row| {
            Ok(PrRow {
                repo: row.get(0)?,
                number: row.get(1)?,
                title: row.get(2)?,
                url: row.get(3)?,
                updated_at: row.get(4)?,
                hidden: row.get::<_, i64>(5)? != 0,
                is_draft: row.get::<_, i64>(6)? != 0,
                head_ref_name: row.get(7)?,
                base_ref_name: row.get(8)?,
                review_status: row.get(9)?,
                reviewers: row.get(10)?,
                checks_overall: row.get(11)?,
                checks_running: row.get::<_, i64>(12)? != 0,
                drci_status: row.get(13)?,
                drci_emoji: row.get(14)?,
                comment_count: row.get(15)?,
                checks_success: row.get(16)?,
                checks_fail: row.get(17)?,
                checks_pending: row.get(18)?,
                landing_status: row.get(19)?,
                head_sha: row.get(20)?,
                base_sha: row.get(21)?,
                checks_queued: row.get(22)?,
            })
        },
    )
    .ok()
}

pub fn hidden_count(conn: &Connection, user: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM prs WHERE target_user = ?1 AND hidden = 1",
        rusqlite::params![user],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

pub fn set_hidden(conn: &Connection, repo: &str, number: i64, hidden: i64, user: &str) {
    conn.execute(
        "UPDATE prs SET hidden = ?1 WHERE target_user = ?2 AND repo = ?3 AND number = ?4",
        rusqlite::params![hidden, user, repo, number],
    )
    .ok();
}

pub fn upsert_review_prs(conn: &Connection, prs: &[PrInsert], user: &str) -> Result<(), rusqlite::Error> {
    // Upsert: never delete on absence. Read state, detail columns
    // (checks_success/fail/pending), and existing drci_emoji/drci_status all
    // survive naturally because rows aren't deleted. drci_emoji and drci_status
    // are guarded with CASE WHEN since the lighter review-PR fetch doesn't
    // populate them (those come from the detail fetcher).
    let mut stmt = conn.prepare(
        "INSERT INTO review_prs (target_user, number, repo, title, url, state, created_at, updated_at,
                                  author, is_draft, head_ref_name, base_ref_name,
                                  review_status, reviewers, checks_overall, checks_running,
                                  drci_status, drci_emoji, comment_count, head_sha, base_sha,
                                  ci_approval_needed, re_review_requested)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
         ON CONFLICT(target_user, repo, number) DO UPDATE SET
           title = excluded.title,
           url = excluded.url,
           state = excluded.state,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at,
           author = excluded.author,
           is_draft = excluded.is_draft,
           head_ref_name = excluded.head_ref_name,
           base_ref_name = excluded.base_ref_name,
           review_status = excluded.review_status,
           reviewers = excluded.reviewers,
           checks_overall = excluded.checks_overall,
           checks_running = excluded.checks_running,
           drci_status = CASE WHEN excluded.drci_status != '' THEN excluded.drci_status ELSE review_prs.drci_status END,
           drci_emoji = CASE WHEN excluded.drci_emoji != '' THEN excluded.drci_emoji ELSE review_prs.drci_emoji END,
           comment_count = excluded.comment_count,
           head_sha = excluded.head_sha,
           base_sha = excluded.base_sha,
           ci_approval_needed = excluded.ci_approval_needed,
           re_review_requested = excluded.re_review_requested,
           -- When CI first needs your approval (false -> true), surface the PR
           -- by marking it unread. References to review_prs columns here are the
           -- pre-update values; excluded.* are the incoming ones.
           is_read = CASE
               WHEN excluded.ci_approval_needed = 1 AND ci_approval_needed = 0
               THEN 0 ELSE is_read END",
    )?;
    for pr in prs {
        stmt.execute(rusqlite::params![
            user,
            pr.number, pr.repo, pr.title, pr.url, pr.state, pr.created_at, pr.updated_at,
            pr.author, pr.is_draft as i64, pr.head_ref_name, pr.base_ref_name,
            pr.review_status, pr.reviewers, pr.checks_overall, pr.checks_running as i64,
            pr.drci_status, pr.drci_emoji, pr.comment_count, pr.head_sha, pr.base_sha,
            pr.ci_approval_needed as i64, pr.re_review_requested as i64,
        ])?;
    }

    // Auto-unread: two independent triggers:
    // 1. PR actually updated (updated_at changed) AND a tracked field changed.
    //    Guarded on updated_at so API-level jitter doesn't cause spurious unreads.
    // 2. DrCI transitioned to passing (regardless of updated_at), since the
    //    reviewer cares that CI is now green even if the PR wasn't otherwise touched.
    auto_unread_with_logging(conn, user)?;

    Ok(())
}

fn auto_unread_with_logging(conn: &Connection, user: &str) -> Result<(), rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT target_user, repo, number,
                updated_at, read_updated_at,
                comment_count, read_comment_count,
                review_status, read_review_status,
                head_sha, read_head_sha,
                title, read_title,
                drci_emoji, read_drci_emoji
         FROM review_prs
         WHERE is_read = 1 AND (
            (updated_at != read_updated_at AND (
                comment_count != read_comment_count
                OR review_status != read_review_status
                OR head_sha != read_head_sha
                OR title != read_title
            ))
            OR (drci_emoji = 'white_check_mark' AND read_drci_emoji != 'white_check_mark' AND read_drci_emoji != '')
         )"
    )?;
    let rows: Vec<(String, String, i64, String)> = stmt.query_map([], |row| {
        let tu: String = row.get(0)?;
        let repo: String = row.get(1)?;
        let number: i64 = row.get(2)?;
        let updated_at: String = row.get(3)?;
        let read_updated_at: String = row.get(4)?;
        let comment_count: i64 = row.get(5)?;
        let read_comment_count: i64 = row.get(6)?;
        let review_status: String = row.get(7)?;
        let read_review_status: String = row.get(8)?;
        let head_sha: String = row.get(9)?;
        let read_head_sha: String = row.get(10)?;
        let title: String = row.get(11)?;
        let read_title: String = row.get(12)?;
        let drci_emoji: String = row.get(13)?;
        let read_drci_emoji: String = row.get(14)?;

        let short = |s: &str| s.chars().take(7).collect::<String>();
        let mut reasons: Vec<String> = Vec::new();
        if updated_at != read_updated_at {
            if comment_count != read_comment_count {
                reasons.push(format!("comment_count {}->{}", read_comment_count, comment_count));
            }
            if review_status != read_review_status {
                reasons.push(format!("review_status {:?}->{:?}", read_review_status, review_status));
            }
            if head_sha != read_head_sha {
                reasons.push(format!("head_sha {}->{}", short(&read_head_sha), short(&head_sha)));
            }
            if title != read_title {
                reasons.push(format!("title {:?}->{:?}", read_title, title));
            }
            if !reasons.is_empty() {
                reasons.insert(0, format!("updated_at {:?}->{:?}", read_updated_at, updated_at));
            }
        }
        if drci_emoji == "white_check_mark"
            && read_drci_emoji != "white_check_mark"
            && !read_drci_emoji.is_empty()
        {
            reasons.push(format!("drci {:?}->white_check_mark", read_drci_emoji));
        }
        Ok((tu, repo, number, reasons.join(", ")))
    })?
    .filter_map(|r| r.ok())
    .collect();

    for (tu, repo, number, reason) in &rows {
        log!(
            "[{}] Auto-unread {}/#{} (upsert by [{}]): {}",
            tu, repo, number, user, reason
        );
    }

    if !rows.is_empty() {
        conn.execute_batch(
            "UPDATE review_prs SET is_read = 0
             WHERE is_read = 1 AND (
                (updated_at != read_updated_at AND (
                    comment_count != read_comment_count
                    OR review_status != read_review_status
                    OR head_sha != read_head_sha
                    OR title != read_title
                ))
                OR (drci_emoji = 'white_check_mark' AND read_drci_emoji != 'white_check_mark' AND read_drci_emoji != '')
             )"
        )?;
    }
    Ok(())
}

pub fn delete_review_pr(conn: &Connection, repo: &str, number: i64, user: &str) {
    conn.execute(
        "DELETE FROM review_prs WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
        rusqlite::params![user, repo, number],
    ).ok();
}

pub fn list_review_prs(conn: &Connection, user: &str) -> Vec<ReviewPrRow> {
    let mut stmt = conn.prepare(
        "SELECT repo, number, title, url, author, is_draft, head_ref_name, base_ref_name, is_read,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count, head_sha,
                updated_at, ci_approval_needed,
                checks_success, checks_fail, checks_pending, base_sha,
                (is_mentioned OR mention_count > last_mention_count),
                checks_queued, re_review_requested, drci_updated_at
         FROM review_prs WHERE target_user = ?1
         ORDER BY (is_mentioned OR mention_count > last_mention_count) DESC,
                  ci_approval_needed DESC, updated_at DESC",
    ).unwrap();
    stmt.query_map(rusqlite::params![user], |row| {
        Ok(ReviewPrRow {
            repo: row.get(0)?,
            number: row.get(1)?,
            title: row.get(2)?,
            url: row.get(3)?,
            author: row.get(4)?,
            is_draft: row.get::<_, i64>(5)? != 0,
            head_ref_name: row.get(6)?,
            base_ref_name: row.get(7)?,
            is_read: row.get::<_, i64>(8)? != 0,
            review_status: row.get(9)?,
            reviewers: row.get(10)?,
            checks_overall: row.get(11)?,
            checks_running: row.get::<_, i64>(12)? != 0,
            drci_status: row.get(13)?,
            drci_emoji: row.get(14)?,
            comment_count: row.get(15)?,
            head_sha: row.get(16)?,
            updated_at: row.get(17)?,
            ci_approval_needed: row.get::<_, i64>(18)? != 0,
            checks_success: row.get(19)?,
            checks_fail: row.get(20)?,
            checks_pending: row.get(21)?,
            base_sha: row.get(22)?,
            is_mentioned: row.get::<_, i64>(23)? != 0,
            checks_queued: row.get(24)?,
            re_review_requested: row.get::<_, i64>(25)? != 0,
            drci_updated_at: row.get(26)?,
        })
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

pub fn set_review_read(conn: &Connection, repo: &str, number: i64, read: bool, user: &str) {
    if read {
        // Mark read and snapshot current values. Also clear mention state
        // and snapshot mention_count so future mentions are detected as new.
        conn.execute(
            "UPDATE review_prs SET is_read = 1,
                read_comment_count = comment_count,
                read_review_status = review_status,
                read_head_sha = head_sha,
                read_title = title,
                read_updated_at = updated_at,
                read_drci_emoji = drci_emoji,
                is_mentioned = 0,
                last_mention_count = mention_count
             WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
            rusqlite::params![user, repo, number],
        ).ok();
    } else {
        conn.execute(
            "UPDATE review_prs SET is_read = 0
             WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
            rusqlite::params![user, repo, number],
        ).ok();
    }
}

pub fn set_review_mention(conn: &Connection, repo: &str, number: i64, mentioned: bool, user: &str) {
    if mentioned {
        // Manual on: just flip the bit. Don't touch last_mention_count so a
        // subsequent dismiss + new mention can still auto-fire.
        conn.execute(
            "UPDATE review_prs SET is_mentioned = 1
             WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
            rusqlite::params![user, repo, number],
        ).ok();
    } else {
        // Manual off: snapshot the current mention_count so we only re-fire
        // when more mentions arrive.
        conn.execute(
            "UPDATE review_prs SET is_mentioned = 0,
                last_mention_count = mention_count
             WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
            rusqlite::params![user, repo, number],
        ).ok();
    }
}

pub fn get_review_pr(conn: &Connection, repo: &str, number: i64, user: &str) -> Option<ReviewPrRow> {
    conn.query_row(
        "SELECT repo, number, title, url, author, is_draft, head_ref_name, base_ref_name, is_read,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count, head_sha,
                updated_at, ci_approval_needed,
                checks_success, checks_fail, checks_pending, base_sha,
                (is_mentioned OR mention_count > last_mention_count),
                checks_queued, re_review_requested, drci_updated_at
         FROM review_prs WHERE target_user = ?1 AND repo = ?2 AND number = ?3",
        rusqlite::params![user, repo, number],
        |row| {
            Ok(ReviewPrRow {
                repo: row.get(0)?,
                number: row.get(1)?,
                title: row.get(2)?,
                url: row.get(3)?,
                author: row.get(4)?,
                is_draft: row.get::<_, i64>(5)? != 0,
                head_ref_name: row.get(6)?,
                base_ref_name: row.get(7)?,
                is_read: row.get::<_, i64>(8)? != 0,
                review_status: row.get(9)?,
                reviewers: row.get(10)?,
                checks_overall: row.get(11)?,
                checks_running: row.get::<_, i64>(12)? != 0,
                drci_status: row.get(13)?,
                drci_emoji: row.get(14)?,
                comment_count: row.get(15)?,
                head_sha: row.get(16)?,
                updated_at: row.get(17)?,
                ci_approval_needed: row.get::<_, i64>(18)? != 0,
                checks_success: row.get(19)?,
                checks_fail: row.get(20)?,
                checks_pending: row.get(21)?,
                base_sha: row.get(22)?,
                is_mentioned: row.get::<_, i64>(23)? != 0,
                checks_queued: row.get(24)?,
                re_review_requested: row.get::<_, i64>(25)? != 0,
                drci_updated_at: row.get(26)?,
            })
        },
    )
    .ok()
}

/// Returns PRs whose details need refreshing: either never fetched, or fetched
/// >max_age_secs ago AND (updated_at changed since OR CI is still pending).
/// The CI-pending clause matters because check-run transitions (and CI finally
/// finishing) don't bump the PR's updated_at, so without it a frozen updated_at
/// would leave the CI pill stuck on "pending" forever. checks_overall is the
/// rollup state, refreshed on every poll by the light query.
pub fn list_stale_prs(conn: &Connection, user: &str, max_age_secs: i64) -> Vec<(String, i64)> {
    let mut stmt = conn.prepare(
        "SELECT repo, number FROM prs
         WHERE target_user = ?1 AND hidden = 0
           AND (detail_updated_at = ''
                OR ((strftime('%s','now') - strftime('%s', detail_updated_at)) > ?2
                    AND (detail_for_updated_at != updated_at OR checks_overall = 'PENDING')))"
    ).unwrap();
    stmt.query_map(rusqlite::params![user, max_age_secs], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

/// Returns review PRs whose details need refreshing. See `list_stale_prs` for
/// why a still-pending CI rollup forces a refresh independent of updated_at.
pub fn list_stale_review_prs(conn: &Connection, user: &str, max_age_secs: i64) -> Vec<(String, i64)> {
    let mut stmt = conn.prepare(
        "SELECT repo, number FROM review_prs
         WHERE target_user = ?1
           AND (detail_updated_at = ''
                OR ((strftime('%s','now') - strftime('%s', detail_updated_at)) > ?2
                    AND (detail_for_updated_at != updated_at OR checks_overall = 'PENDING')))"
    ).unwrap();
    stmt.query_map(rusqlite::params![user, max_age_secs], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

pub struct PrDetailUpdate {
    pub checks_success: i64,
    pub checks_fail: i64,
    pub checks_pending: i64,
    pub checks_queued: i64,
    pub checks_running: bool,
    pub drci_emoji: String,
    pub drci_status: String,
    pub drci_updated_at: String,
    pub landing_status: String,
    pub mention_count: i64,
}

pub fn update_pr_details(conn: &Connection, repo: &str, number: i64, user: &str, d: &PrDetailUpdate) {
    conn.execute(
        "UPDATE prs SET
            checks_success = ?1, checks_fail = ?2, checks_pending = ?3, checks_running = ?4,
            drci_emoji = ?5, drci_status = ?6, landing_status = ?7,
            checks_queued = ?8,
            detail_updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            detail_for_updated_at = updated_at
         WHERE target_user = ?9 AND repo = ?10 AND number = ?11",
        rusqlite::params![
            d.checks_success, d.checks_fail, d.checks_pending, d.checks_running as i64,
            d.drci_emoji, d.drci_status, d.landing_status,
            d.checks_queued,
            user, repo, number,
        ],
    ).ok();
}

pub fn update_review_pr_details(conn: &Connection, repo: &str, number: i64, user: &str, d: &PrDetailUpdate) {
    conn.execute(
        // Only record the raw mention_count here. Whether the PR counts as
        // "mentioned" is derived on read as (manual is_mentioned bit) OR
        // (mention_count > last_mention_count) — see list_review_prs/get_review_pr.
        // Deriving it means an edited/deleted @-mention (mention_count drops)
        // clears the pill instead of leaving the auto-set bit stuck on.
        "UPDATE review_prs SET
            checks_success = ?1, checks_fail = ?2, checks_pending = ?3, checks_running = ?4,
            drci_emoji = ?5, drci_status = ?6,
            mention_count = ?7,
            checks_queued = ?8,
            drci_updated_at = ?12,
            detail_updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            detail_for_updated_at = updated_at
         WHERE target_user = ?9 AND repo = ?10 AND number = ?11",
        rusqlite::params![
            d.checks_success, d.checks_fail, d.checks_pending, d.checks_running as i64,
            d.drci_emoji, d.drci_status, d.mention_count,
            d.checks_queued,
            user, repo, number,
            d.drci_updated_at,
        ],
    ).ok();
}

pub fn replace_merged_prs(conn: &Connection, prs: &[MergedPrRow], user: &str) -> Result<(), rusqlite::Error> {
    conn.execute("DELETE FROM merged_prs WHERE target_user = ?1", rusqlite::params![user])?;
    let mut stmt = conn.prepare(
        "INSERT INTO merged_prs (target_user, number, repo, title, url, landed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for pr in prs {
        stmt.execute(rusqlite::params![
            user, pr.number, pr.repo, pr.title, pr.url, pr.landed_at,
        ])?;
    }
    Ok(())
}

pub fn list_merged_prs(conn: &Connection, user: &str) -> Vec<MergedPrRow> {
    let mut stmt = conn.prepare(
        "SELECT repo, number, title, url, landed_at
         FROM merged_prs WHERE target_user = ?1
         ORDER BY landed_at DESC",
    ).unwrap();
    stmt.query_map(rusqlite::params![user], |row| {
        Ok(MergedPrRow {
            repo: row.get(0)?,
            number: row.get(1)?,
            title: row.get(2)?,
            url: row.get(3)?,
            landed_at: row.get(4)?,
        })
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

pub fn upsert_issues(conn: &Connection, issues: &[IssueInsert], user: &str) -> Result<(), rusqlite::Error> {
    let mut stmt = conn.prepare(
        "INSERT INTO issues (target_user, number, repo, title, url, author, created_at, updated_at, comment_count, labels)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(target_user, repo, number) DO UPDATE SET
           title = excluded.title,
           url = excluded.url,
           author = excluded.author,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at,
           comment_count = excluded.comment_count,
           labels = excluded.labels",
    )?;
    for issue in issues {
        stmt.execute(rusqlite::params![
            user, issue.number, issue.repo, issue.title, issue.url,
            issue.author, issue.created_at, issue.updated_at, issue.comment_count, issue.labels,
        ])?;
    }
    Ok(())
}

pub fn list_issues(conn: &Connection, user: &str) -> Vec<IssueRow> {
    let mut stmt = conn.prepare(
        "SELECT repo, number, title, url, author, created_at, updated_at, comment_count, labels
         FROM issues WHERE target_user = ?1
         ORDER BY updated_at DESC",
    ).unwrap();
    stmt.query_map(rusqlite::params![user], |row| {
        Ok(IssueRow {
            repo: row.get(0)?,
            number: row.get(1)?,
            title: row.get(2)?,
            url: row.get(3)?,
            author: row.get(4)?,
            created_at: row.get(5)?,
            updated_at: row.get(6)?,
            comment_count: row.get(7)?,
            labels: row.get(8)?,
        })
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn, 0);
        conn
    }

    /// Insert a review PR with explicit detail-staleness fields. `updated_at`
    /// and `detail_for_updated_at` are set equal so the "PR changed" half of the
    /// stale gate is false — isolating the CI-still-pending behavior.
    fn insert_review_pr(
        conn: &Connection,
        number: i64,
        detail_updated_at: &str,
        checks_overall: &str,
    ) {
        conn.execute(
            "INSERT INTO review_prs
                (target_user, number, repo, title, url, state, created_at, updated_at,
                 detail_updated_at, detail_for_updated_at, checks_overall)
             VALUES (?1, ?2, ?3, ?4, ?5, 'OPEN', '2026-01-01T00:00:00Z',
                     '2026-06-02T00:24:50Z', ?6, '2026-06-02T00:24:50Z', ?7)",
            rusqlite::params![
                "me", number, "pytorch/pytorch", "t", "u",
                detail_updated_at, checks_overall,
            ],
        ).unwrap();
    }

    fn detail(mention_count: i64) -> PrDetailUpdate {
        PrDetailUpdate {
            checks_success: 0,
            checks_fail: 0,
            checks_pending: 0,
            checks_queued: 0,
            checks_running: false,
            drci_emoji: String::new(),
            drci_status: String::new(),
            drci_updated_at: String::new(),
            landing_status: String::new(),
            mention_count,
        }
    }

    fn is_mentioned(conn: &Connection, number: i64) -> bool {
        get_review_pr(conn, "pytorch/pytorch", number, "me").unwrap().is_mentioned
    }

    fn is_read(conn: &Connection, number: i64) -> bool {
        get_review_pr(conn, "pytorch/pytorch", number, "me").unwrap().is_read
    }

    fn review_insert(number: i64, ci_approval_needed: bool, updated_at: &str) -> PrInsert {
        PrInsert {
            number,
            repo: "pytorch/pytorch".to_string(),
            title: "t".to_string(),
            url: "u".to_string(),
            state: "OPEN".to_string(),
            created_at: String::new(),
            updated_at: updated_at.to_string(),
            author: "a".to_string(),
            is_draft: false,
            head_ref_name: String::new(),
            base_ref_name: String::new(),
            review_status: String::new(),
            reviewers: "[]".to_string(),
            checks_overall: String::new(),
            checks_running: false,
            drci_status: String::new(),
            drci_emoji: String::new(),
            comment_count: 0,
            head_sha: String::new(),
            ci_approval_needed,
            base_sha: String::new(),
            re_review_requested: false,
        }
    }

    #[test]
    fn newly_needed_ci_approval_marks_a_read_pr_unread() {
        // When CI first needs your approval, surface the PR by marking it unread.
        let conn = test_db();
        upsert_review_prs(&conn, &[review_insert(167224, false, "t1")], "me").unwrap();
        set_review_read(&conn, "pytorch/pytorch", 167224, true, "me");
        assert!(is_read(&conn, 167224), "precondition: PR is read");

        // CI approval becomes needed (updated_at also advances, as in production).
        upsert_review_prs(&conn, &[review_insert(167224, true, "t2")], "me").unwrap();
        assert!(!is_read(&conn, 167224),
            "newly-needed CI approval should mark the PR unread");
    }

    #[test]
    fn unchanged_ci_approval_does_not_touch_read_state() {
        let conn = test_db();
        // Already needs approval and you've read it: must stay read.
        upsert_review_prs(&conn, &[review_insert(2, true, "t1")], "me").unwrap();
        set_review_read(&conn, "pytorch/pytorch", 2, true, "me");
        upsert_review_prs(&conn, &[review_insert(2, true, "t2")], "me").unwrap();
        assert!(is_read(&conn, 2), "an already-needed CI approval should not re-unread");

        // Never needed approval: must stay read.
        upsert_review_prs(&conn, &[review_insert(3, false, "t1")], "me").unwrap();
        set_review_read(&conn, "pytorch/pytorch", 3, true, "me");
        upsert_review_prs(&conn, &[review_insert(3, false, "t2")], "me").unwrap();
        assert!(is_read(&conn, 3), "no CI-approval transition should change read state");
    }

    #[test]
    fn auto_mention_clears_when_the_mention_disappears() {
        // Repro for #167224: a comment @-mentions you (mention_count rises), then
        // that comment is edited/deleted (mention_count falls). The pill must not
        // stay stuck.
        let conn = test_db();
        insert_review_pr(&conn, 167224, "2020-01-01T00:00:00Z", "SUCCESS");

        update_review_pr_details(&conn, "pytorch/pytorch", 167224, "me", &detail(1));
        assert!(is_mentioned(&conn, 167224), "a new mention should light up the pill");

        update_review_pr_details(&conn, "pytorch/pytorch", 167224, "me", &detail(0));
        assert!(!is_mentioned(&conn, 167224),
            "once the mention is gone the pill must clear");
    }

    #[test]
    fn manual_mention_survives_detail_refresh_with_no_mentions() {
        // "Mark mention" is an explicit override and must not be wiped by the
        // next detail fetch that finds zero @-mentions.
        let conn = test_db();
        insert_review_pr(&conn, 1, "2020-01-01T00:00:00Z", "SUCCESS");

        set_review_mention(&conn, "pytorch/pytorch", 1, true, "me");
        update_review_pr_details(&conn, "pytorch/pytorch", 1, "me", &detail(0));
        assert!(is_mentioned(&conn, 1), "a manual mention should persist");
    }

    #[test]
    fn reading_acknowledges_mention_and_does_not_refire_for_same_count() {
        let conn = test_db();
        insert_review_pr(&conn, 2, "2020-01-01T00:00:00Z", "SUCCESS");

        update_review_pr_details(&conn, "pytorch/pytorch", 2, "me", &detail(1));
        assert!(is_mentioned(&conn, 2));

        set_review_read(&conn, "pytorch/pytorch", 2, true, "me");
        assert!(!is_mentioned(&conn, 2), "marking read clears the mention");

        // Same mention still present on the next fetch — must stay acknowledged.
        update_review_pr_details(&conn, "pytorch/pytorch", 2, "me", &detail(1));
        assert!(!is_mentioned(&conn, 2), "an already-read mention should not refire");

        // A genuinely new mention re-fires.
        update_review_pr_details(&conn, "pytorch/pytorch", 2, "me", &detail(2));
        assert!(is_mentioned(&conn, 2), "an additional mention should refire");
    }

    #[test]
    fn migration_clears_stale_mention_bit() {
        // A pre-existing DB can have is_mentioned stuck on from the old auto-set
        // (mention edited/deleted -> count dropped but bit never cleared). The
        // 22->23 migration must reset it. Build a v22 DB, plant a stuck row, then
        // migrate.
        let conn = Connection::open_in_memory().unwrap();
        for (i, m) in MIGRATIONS[0..22].iter().enumerate() {
            // Mirror run_migrations' special-case: the 6->7 is_draft ALTER is a
            // no-op on DBs whose initial schema already had the column.
            if i == 6 && has_column(&conn, "prs", "is_draft") {
                continue;
            }
            conn.execute_batch(m).unwrap();
        }
        conn.execute_batch("PRAGMA user_version = 22").unwrap();
        conn.execute(
            "INSERT INTO review_prs
                (target_user, number, repo, title, url, state, created_at, updated_at,
                 is_mentioned, mention_count, last_mention_count)
             VALUES ('me', 167224, 'pytorch/pytorch', 't', 'u', 'OPEN', '', '', 1, 0, 0)",
            [],
        ).unwrap();

        run_migrations(&conn, 22);

        assert!(!is_mentioned(&conn, 167224),
            "the 22->23 migration should clear a stuck auto-set mention bit");
    }

    #[test]
    fn migrations_run_through_latest_version() {
        // Guards against CURRENT_VERSION drifting behind the MIGRATIONS array:
        // a missing migration leaves the schema short a column, and the SELECTs
        // below then panic at runtime (poisoning the db mutex in production).
        let conn = test_db();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version as usize, MIGRATIONS.len(),
            "every migration in MIGRATIONS must run");
        assert!(has_column(&conn, "review_prs", "drci_updated_at"));
    }

    #[test]
    fn review_pr_queries_match_schema() {
        // Exercises the full SELECT column lists; panics if a referenced column
        // (e.g. drci_updated_at) wasn't added by a migration.
        let conn = test_db();
        let _ = list_review_prs(&conn, "me");
        let _ = get_review_pr(&conn, "pytorch/pytorch", 1, "me");
        let _ = list_prs(&conn, true, "me");
    }

    #[test]
    fn stale_review_pr_refetches_while_ci_pending_even_if_updated_at_unchanged() {
        // Repro for #176044: CI checks transition (and eventually finish) without
        // bumping the PR's updated_at, so the "updated_at changed" gate alone
        // freezes the CI pill. While the rollup is still PENDING we must keep
        // refreshing details regardless.
        let conn = test_db();
        insert_review_pr(&conn, 176044, "2020-01-01T00:00:00Z", "PENDING");

        let stale = list_stale_review_prs(&conn, "me", 60);
        assert!(
            stale.iter().any(|(_, n)| *n == 176044),
            "a PR with a still-pending CI rollup should be refetched even when updated_at is frozen"
        );
    }

    #[test]
    fn terminal_ci_pr_not_refetched_when_updated_at_unchanged() {
        // Counterpart: once CI has settled (rollup SUCCESS) and the PR itself
        // hasn't changed, there's nothing to refetch.
        let conn = test_db();
        insert_review_pr(&conn, 200, "2020-01-01T00:00:00Z", "SUCCESS");

        let stale = list_stale_review_prs(&conn, "me", 60);
        assert!(
            !stale.iter().any(|(_, n)| *n == 200),
            "a settled PR with unchanged updated_at should not be refetched"
        );
    }

    #[test]
    fn recently_fetched_pending_pr_not_refetched_before_max_age() {
        // Even pending PRs respect max_age — don't hammer details every cycle.
        let conn = test_db();
        insert_review_pr(&conn, 300, "2026-06-02T03:00:00Z", "PENDING");

        // 1-hour max_age; detail was fetched at 03:00, "now" is later but the
        // test only asserts it's excluded when within max_age. Use a huge
        // max_age so the age check is definitely not exceeded.
        let stale = list_stale_review_prs(&conn, "me", 10_000_000_000);
        assert!(
            !stale.iter().any(|(_, n)| *n == 300),
            "a recently-fetched PR should not be refetched until max_age elapses"
        );
    }
}
