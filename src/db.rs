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
    pub drci_status: String,
    pub drci_emoji: String,
    pub comment_count: i64,
    pub landing_status: String,
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
    pub drci_status: String,
    pub drci_emoji: String,
    pub comment_count: i64,
    pub head_sha: String,
    pub updated_at: String,
    pub ci_approval_needed: bool,
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

const CURRENT_VERSION: i64 = 14;

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
            eprintln!("Migrating legacy database to versioned schema");
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
        eprintln!("Running migration {} -> {}", v, v + 1);

        // Migration 6->7 adds is_draft, but some v6 DBs already have it
        if v == 6 && has_column(conn, "prs", "is_draft") {
            eprintln!("  is_draft column already exists, skipping ALTER");
        } else {
            conn.execute_batch(MIGRATIONS[idx])
                .unwrap_or_else(|e| panic!("Migration {} -> {} failed: {}", v, v + 1, e));
        }

        conn.execute_batch(&format!("PRAGMA user_version = {}", v + 1))
            .expect("Failed to set user_version");
    }
}

pub fn replace_prs(conn: &Connection, prs: &[PrInsert], user: &str) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS pr_preserve AS
         SELECT target_user, repo, number, hidden, checks_success, checks_fail, checks_pending,
                landing_status, detail_updated_at
         FROM prs"
    )?;
    conn.execute("DELETE FROM prs WHERE target_user = ?1", rusqlite::params![user])?;
    let mut stmt = conn.prepare(
        "INSERT INTO prs (target_user, number, repo, title, url, state, created_at, updated_at, hidden,
                          is_draft, head_ref_name, base_ref_name,
                          review_status, reviewers, checks_overall, checks_running,
                          drci_status, drci_emoji, comment_count,
                          checks_success, checks_fail, checks_pending, landing_status, detail_updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                 COALESCE((SELECT hidden FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), 0),
                 ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18,
                 COALESCE((SELECT checks_success FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), 0),
                 COALESCE((SELECT checks_fail FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), 0),
                 COALESCE((SELECT checks_pending FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), 0),
                 COALESCE((SELECT landing_status FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), ''),
                 COALESCE((SELECT detail_updated_at FROM temp.pr_preserve WHERE target_user = ?1 AND repo = ?3 AND number = ?2), ''))",
    )?;
    for pr in prs {
        stmt.execute(rusqlite::params![
            user,
            pr.number, pr.repo, pr.title, pr.url, pr.state, pr.created_at, pr.updated_at,
            pr.is_draft as i64, pr.head_ref_name, pr.base_ref_name,
            pr.review_status, pr.reviewers, pr.checks_overall, pr.checks_running as i64,
            pr.drci_status, pr.drci_emoji, pr.comment_count,
        ])?;
    }
    conn.execute_batch("DROP TABLE IF EXISTS temp.pr_preserve")?;
    Ok(())
}

pub fn list_prs(conn: &Connection, show_hidden: bool, user: &str) -> Vec<PrRow> {
    let sql = if show_hidden {
        "SELECT repo, number, title, url, updated_at, hidden, is_draft, head_ref_name, base_ref_name,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count,
                checks_success, checks_fail, checks_pending, landing_status
         FROM prs WHERE target_user = ?1 ORDER BY updated_at DESC"
    } else {
        "SELECT repo, number, title, url, updated_at, hidden, is_draft, head_ref_name, base_ref_name,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count,
                checks_success, checks_fail, checks_pending, landing_status
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
                checks_success, checks_fail, checks_pending, landing_status
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

pub fn replace_review_prs(conn: &Connection, prs: &[PrInsert], user: &str) -> Result<(), rusqlite::Error> {
    // Preserve read state and detail data from old data
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS review_preserve AS
         SELECT target_user, repo, number, is_read, read_comment_count, read_review_status, read_head_sha, read_title,
                checks_success, checks_fail, checks_pending, drci_emoji, drci_status, detail_updated_at
         FROM review_prs"
    )?;

    conn.execute("DELETE FROM review_prs WHERE target_user = ?1", rusqlite::params![user])?;
    let mut stmt = conn.prepare(
        "INSERT INTO review_prs (target_user, number, repo, title, url, state, created_at, updated_at,
                                  author, is_draft, head_ref_name, base_ref_name,
                                  review_status, reviewers, checks_overall, checks_running,
                                  drci_status, drci_emoji, comment_count, head_sha,
                                  ci_approval_needed)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
    )?;
    for pr in prs {
        stmt.execute(rusqlite::params![
            user,
            pr.number, pr.repo, pr.title, pr.url, pr.state, pr.created_at, pr.updated_at,
            pr.author, pr.is_draft as i64, pr.head_ref_name, pr.base_ref_name,
            pr.review_status, pr.reviewers, pr.checks_overall, pr.checks_running as i64,
            pr.drci_status, pr.drci_emoji, pr.comment_count, pr.head_sha,
            pr.ci_approval_needed as i64,
        ])?;
    }

    // Restore detail data for all PRs that still exist
    conn.execute_batch(
        "UPDATE review_prs SET
            checks_success = rp.checks_success,
            checks_fail = rp.checks_fail,
            checks_pending = rp.checks_pending,
            drci_emoji = CASE WHEN rp.drci_emoji != '' THEN rp.drci_emoji ELSE review_prs.drci_emoji END,
            drci_status = CASE WHEN rp.drci_status != '' THEN rp.drci_status ELSE review_prs.drci_status END,
            detail_updated_at = rp.detail_updated_at
         FROM temp.review_preserve rp
         WHERE review_prs.target_user = rp.target_user
           AND review_prs.repo = rp.repo
           AND review_prs.number = rp.number"
    )?;

    // Restore read state for read PRs
    conn.execute_batch(
        "UPDATE review_prs SET
            is_read = 1,
            read_comment_count = rp.read_comment_count,
            read_review_status = rp.read_review_status,
            read_head_sha = rp.read_head_sha,
            read_title = rp.read_title
         FROM temp.review_preserve rp
         WHERE review_prs.target_user = rp.target_user
           AND review_prs.repo = rp.repo
           AND review_prs.number = rp.number
           AND rp.is_read = 1"
    )?;

    // Auto-unread: if tracked fields changed since last mark-read
    conn.execute_batch(
        "UPDATE review_prs SET is_read = 0
         WHERE is_read = 1 AND (
            comment_count != read_comment_count
            OR review_status != read_review_status
            OR head_sha != read_head_sha
            OR title != read_title
         )"
    )?;

    conn.execute_batch("DROP TABLE IF EXISTS temp.review_preserve")?;
    Ok(())
}

pub fn list_review_prs(conn: &Connection, user: &str) -> Vec<ReviewPrRow> {
    let mut stmt = conn.prepare(
        "SELECT repo, number, title, url, author, is_draft, head_ref_name, base_ref_name, is_read,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count, head_sha,
                updated_at, ci_approval_needed,
                checks_success, checks_fail, checks_pending
         FROM review_prs WHERE target_user = ?1
         ORDER BY ci_approval_needed DESC, updated_at DESC",
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
        })
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

pub fn set_review_read(conn: &Connection, repo: &str, number: i64, read: bool, user: &str) {
    if read {
        // Mark read and snapshot current values
        conn.execute(
            "UPDATE review_prs SET is_read = 1,
                read_comment_count = comment_count,
                read_review_status = review_status,
                read_head_sha = head_sha,
                read_title = title
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

pub fn get_review_pr(conn: &Connection, repo: &str, number: i64, user: &str) -> Option<ReviewPrRow> {
    conn.query_row(
        "SELECT repo, number, title, url, author, is_draft, head_ref_name, base_ref_name, is_read,
                review_status, reviewers, checks_overall, checks_running,
                drci_status, drci_emoji, comment_count, head_sha,
                updated_at, ci_approval_needed,
                checks_success, checks_fail, checks_pending
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
            })
        },
    )
    .ok()
}

/// Returns PRs whose details haven't been fetched in the last `max_age_secs` seconds.
pub fn list_stale_prs(conn: &Connection, user: &str, max_age_secs: i64) -> Vec<(String, i64)> {
    let mut stmt = conn.prepare(
        "SELECT repo, number FROM prs
         WHERE target_user = ?1 AND hidden = 0
           AND (detail_updated_at = ''
                OR (strftime('%s','now') - strftime('%s', detail_updated_at)) > ?2)"
    ).unwrap();
    stmt.query_map(rusqlite::params![user, max_age_secs], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })
    .unwrap()
    .filter_map(|r| r.ok())
    .collect()
}

/// Returns review PRs whose details haven't been fetched in the last `max_age_secs` seconds.
pub fn list_stale_review_prs(conn: &Connection, user: &str, max_age_secs: i64) -> Vec<(String, i64)> {
    let mut stmt = conn.prepare(
        "SELECT repo, number FROM review_prs
         WHERE target_user = ?1
           AND (detail_updated_at = ''
                OR (strftime('%s','now') - strftime('%s', detail_updated_at)) > ?2)"
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
    pub checks_running: bool,
    pub drci_emoji: String,
    pub drci_status: String,
    pub landing_status: String,
}

pub fn update_pr_details(conn: &Connection, repo: &str, number: i64, user: &str, d: &PrDetailUpdate) {
    conn.execute(
        "UPDATE prs SET
            checks_success = ?1, checks_fail = ?2, checks_pending = ?3, checks_running = ?4,
            drci_emoji = ?5, drci_status = ?6, landing_status = ?7,
            detail_updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE target_user = ?8 AND repo = ?9 AND number = ?10",
        rusqlite::params![
            d.checks_success, d.checks_fail, d.checks_pending, d.checks_running as i64,
            d.drci_emoji, d.drci_status, d.landing_status,
            user, repo, number,
        ],
    ).ok();
}

pub fn update_review_pr_details(conn: &Connection, repo: &str, number: i64, user: &str, d: &PrDetailUpdate) {
    conn.execute(
        "UPDATE review_prs SET
            checks_success = ?1, checks_fail = ?2, checks_pending = ?3, checks_running = ?4,
            drci_emoji = ?5, drci_status = ?6,
            detail_updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE target_user = ?7 AND repo = ?8 AND number = ?9",
        rusqlite::params![
            d.checks_success, d.checks_fail, d.checks_pending, d.checks_running as i64,
            d.drci_emoji, d.drci_status,
            user, repo, number,
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

pub fn replace_issues(conn: &Connection, issues: &[IssueInsert], user: &str) -> Result<(), rusqlite::Error> {
    conn.execute("DELETE FROM issues WHERE target_user = ?1", rusqlite::params![user])?;
    let mut stmt = conn.prepare(
        "INSERT INTO issues (target_user, number, repo, title, url, author, created_at, updated_at, comment_count, labels)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
