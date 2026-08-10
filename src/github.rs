use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

use reqwest::Client;
use serde::Deserialize;

use crate::db::PrInsert;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

static CLIENT: OnceLock<(Client, String)> = OnceLock::new();


/// Epoch second when rate limit backoff expires. 0 = not rate limited.
static RATE_LIMITED_UNTIL: AtomicU64 = AtomicU64::new(0);

/// Whether we already logged a "rate limit low" warning this cycle.
static RATE_LOW_LOGGED: AtomicBool = AtomicBool::new(false);

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Returns true if we're currently rate-limited.
pub fn is_rate_limited() -> bool {
    let until = RATE_LIMITED_UNTIL.load(Ordering::Relaxed);
    until > 0 && now_epoch() < until
}

/// Returns how many minutes remain on the backoff, or 0 if not limited.
pub fn rate_limited_minutes_remaining() -> u64 {
    let until = RATE_LIMITED_UNTIL.load(Ordering::Relaxed);
    let now = now_epoch();
    if until > now { (until - now + 59) / 60 } else { 0 }
}

fn check_rate_limited() -> Result<(), BoxErr> {
    if is_rate_limited() {
        let mins = rate_limited_minutes_remaining();
        Err(format!("Rate limited by GitHub — backing off for ~{} more min", mins).into())
    } else {
        Ok(())
    }
}

fn set_rate_limited(resp: &reqwest::Response) {
    // Use x-ratelimit-reset if available, otherwise back off 60 minutes
    let until = resp.headers().get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| now_epoch() + 3600);
    let prev = RATE_LIMITED_UNTIL.swap(until, Ordering::Relaxed);
    // Only log on the first detection (prev was 0 or already expired)
    if prev == 0 || now_epoch() >= prev {
        let mins = (until.saturating_sub(now_epoch()) + 59) / 60;
        log!("RATE LIMITED by GitHub! Backing off for ~{} min (until reset)", mins);
    }
}

/// Call from main() before any GitHub requests to configure the HTTP client.
pub fn init_client(proxy: Option<&str>) {
    CLIENT.get_or_init(|| {
        let (token, source) = resolve_token().expect("Could not find GitHub token. Set GITHUB_TOKEN env var or authenticate with `gh auth login`.");
        log!("GitHub token from {}", source);
        // Per-request timeouts so a hung connection can't wedge a fetch loop
        // indefinitely (it'll error, get logged, and retry next cycle).
        let mut builder = Client::builder()
            .user_agent("prview")
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(45));
        if let Some(proxy_url) = proxy {
            log!("Using proxy: {}", proxy_url);
            builder = builder.proxy(
                reqwest::Proxy::all(proxy_url)
                    .unwrap_or_else(|e| panic!("Invalid proxy URL {:?}: {}", proxy_url, e))
            );
        }
        let client = builder.build().expect("Failed to build HTTP client");
        (client, token)
    });
}

/// Query GitHub for the authenticated user. Call after init_client.
pub async fn whoami() -> String {
    match rest_get("user").await {
        Ok(body) => {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            v.get("login").and_then(|l| l.as_str()).unwrap_or("unknown").to_string()
        }
        Err(e) => format!("error: {}", e),
    }
}

fn get_client() -> &'static (Client, String) {
    CLIENT.get().expect("GitHub client not initialized — call github::init_client() first")
}

fn resolve_token() -> Option<(String, &'static str)> {
    // 1. GITHUB_TOKEN env
    if let Ok(t) = std::env::var("GITHUB_TOKEN") {
        if !t.is_empty() { return Some((t, "GITHUB_TOKEN env")); }
    }
    // 2. GH_TOKEN env
    if let Ok(t) = std::env::var("GH_TOKEN") {
        if !t.is_empty() { return Some((t, "GH_TOKEN env")); }
    }
    // 3. Config files: our own first, then gh's
    let config_dir = std::env::var("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").expect("HOME not set");
            std::path::PathBuf::from(home).join(".config")
        });
    if let Some(token) = read_token_from_hosts_yml(&config_dir.join("prview").join("hosts.yml")) {
        return Some((token, "~/.config/prview/hosts.yml"));
    }
    if let Some(token) = read_token_from_hosts_yml(&config_dir.join("gh").join("hosts.yml")) {
        return Some((token, "~/.config/gh/hosts.yml"));
    }
    None
}

fn read_token_from_hosts_yml(path: &std::path::Path) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let mut in_github = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("github.com") {
            in_github = true;
            continue;
        }
        if in_github && !line.starts_with(' ') && !line.starts_with('\t') {
            break;
        }
        if in_github {
            if let Some(rest) = trimmed.strip_prefix("oauth_token:") {
                let token = rest.trim().to_string();
                if !token.is_empty() { return Some(token); }
            }
        }
    }
    None
}

fn log_rate_limit(resp: &reqwest::Response, label: &str) {
    let hdr = |name: &str| -> Option<i64> {
        resp.headers().get(name)?.to_str().ok()?.parse().ok()
    };
    if let (Some(remaining), Some(limit)) = (hdr("x-ratelimit-remaining"), hdr("x-ratelimit-limit")) {
        if limit > 0 && remaining * 5 < limit {
            // Only log the first low-rate warning per cycle
            if !RATE_LOW_LOGGED.swap(true, Ordering::Relaxed) {
                let resource = resp.headers().get("x-ratelimit-resource")
                    .and_then(|v| v.to_str().ok()).unwrap_or("unknown");
                log!("WARNING: {} rate limit low: {}/{} ({})", label, remaining, limit, resource);
            }
        } else {
            // Rate limit recovered — reset so we log again if it drops
            RATE_LOW_LOGGED.store(false, Ordering::Relaxed);
        }
    }
}

async fn graphql(query: &str) -> Result<Vec<u8>, BoxErr> {
    check_rate_limited()?;
    let (client, token) = get_client();
    let resp = client
        .post("https://api.github.com/graphql")
        .bearer_auth(token)
        .json(&serde_json::json!({"query": query}))
        .send()
        .await?;
    log_rate_limit(&resp, "GraphQL");
    if resp.status() == 403 || resp.status() == 429 {
        if resp.headers().get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok()) == Some(0)
        {
            set_rate_limited(&resp);
        }
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("GitHub rate limit exceeded: {}", body).into());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("GitHub API error ({}): {}", status, body).into());
    }
    Ok(resp.bytes().await?.to_vec())
}

async fn rest_get(endpoint: &str) -> Result<Vec<u8>, BoxErr> {
    check_rate_limited()?;
    let (client, token) = get_client();
    let url = format!("https://api.github.com/{}", endpoint);
    let resp = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?;
    log_rate_limit(&resp, "REST");
    if resp.status() == 403 || resp.status() == 429 {
        if resp.headers().get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok()) == Some(0)
        {
            set_rate_limited(&resp);
        }
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("GitHub rate limit exceeded: {}", body).into());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("GitHub API error ({}): {}", status, body).into());
    }
    Ok(resp.bytes().await?.to_vec())
}

const MY_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  headRefName baseRefName
  baseRef { target { oid } }
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state } }
  commits(last: 1) { nodes { commit {
    oid
    statusCheckRollup { state }
    checkSuites(first: 20) { nodes { status } }
  } } }
  comments(first: 100) { nodes { author { login } body } }
";

const REVIEW_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  additions deletions
  headRefName baseRefName
  baseRef { target { oid } }
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state commit { oid } } }
  reviewRequests(first: 20) { nodes { requestedReviewer { ... on User { login } } } }
  commits(last: 1) { nodes { commit {
    oid
    statusCheckRollup { state }
    checkSuites(first: 20) { nodes { status } }
  } } }
  comments { totalCount }
";

const LANDED_PR_FIELDS: &str = "
  number title url
  repository { nameWithOwner }
  state mergedAt closedAt
  timelineItems(last: 1, itemTypes: [CLOSED_EVENT]) {
    nodes { ... on ClosedEvent { closer { ... on Commit { __typename } } } }
  }
";

// GraphQL response types

#[derive(Deserialize)]
struct GqlResponse {
    data: GqlData,
}

#[derive(Deserialize)]
struct GqlData {
    search: GqlSearch,
}

#[derive(Deserialize)]
struct GqlSearch {
    #[serde(rename = "pageInfo")]
    page_info: GqlPageInfo,
    nodes: Vec<GqlPr>,
}

#[derive(Deserialize)]
struct GqlPageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Deserialize)]
struct GqlPr {
    number: i64,
    title: String,
    url: String,
    author: Option<GqlAuthor>,
    #[serde(default, rename = "isDraft")]
    is_draft: bool,
    #[serde(default)]
    additions: i64,
    #[serde(default)]
    deletions: i64,
    #[serde(default, rename = "headRefName")]
    head_ref_name: String,
    #[serde(default, rename = "baseRefName")]
    base_ref_name: String,
    #[serde(default, rename = "baseRef")]
    base_ref: Option<GqlRef>,
    repository: GqlRepo,
    #[serde(default)]
    state: String,
    #[serde(default, rename = "createdAt")]
    created_at: String,
    #[serde(default, rename = "updatedAt")]
    updated_at: String,
    #[serde(rename = "reviewDecision")]
    review_decision: Option<String>,
    #[serde(default)]
    reviews: GqlNodes<GqlReview>,
    #[serde(default, rename = "reviewRequests")]
    review_requests: GqlNodes<GqlReviewRequest>,
    commits: Option<GqlNodes<GqlCommitNode>>,
    comments: Option<GqlNodes<GqlComment>>,
    #[serde(default, rename = "mergedAt")]
    merged_at: Option<String>,
    #[serde(default, rename = "closedAt")]
    closed_at: Option<String>,
    #[serde(default, rename = "timelineItems")]
    timeline_items: Option<GqlNodes<GqlTimelineItem>>,
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct GqlNodes<T> {
    #[serde(default)]
    nodes: Vec<T>,
    #[serde(default, rename = "totalCount")]
    total_count: Option<i64>,
}

impl<T> Default for GqlNodes<T> {
    fn default() -> Self {
        GqlNodes { nodes: Vec::new(), total_count: None }
    }
}

#[derive(Deserialize)]
struct GqlRepo {
    #[serde(rename = "nameWithOwner")]
    name_with_owner: String,
}

#[derive(Deserialize)]
struct GqlRefTarget {
    oid: Option<String>,
}

#[derive(Deserialize)]
struct GqlRef {
    target: Option<GqlRefTarget>,
}

#[derive(Deserialize)]
struct GqlReview {
    author: GqlAuthor,
    state: String,
    /// The commit this review was submitted against. Lets us tell when the
    /// author has pushed new commits since the review.
    #[serde(default)]
    commit: Option<GqlReviewCommit>,
}

#[derive(Deserialize)]
struct GqlReviewCommit {
    oid: Option<String>,
}

#[derive(Deserialize)]
struct GqlReviewRequest {
    #[serde(default, rename = "requestedReviewer")]
    requested_reviewer: Option<GqlRequestedReviewer>,
}

#[derive(Deserialize)]
struct GqlRequestedReviewer {
    // Only present for User reviewers; absent for Team/Mannequin requests.
    #[serde(default)]
    login: Option<String>,
}

#[derive(Deserialize)]
struct GqlAuthor {
    login: String,
}

#[derive(Deserialize)]
struct GqlCommitNode {
    commit: GqlCommit,
}

#[derive(Deserialize)]
struct GqlCommit {
    oid: Option<String>,
    #[serde(rename = "statusCheckRollup")]
    status_check_rollup: Option<GqlStatusCheckRollup>,
    #[serde(rename = "checkSuites")]
    check_suites: Option<GqlNodes<GqlCheckSuite>>,
}

#[derive(Deserialize)]
struct GqlStatusCheckRollup {
    state: Option<String>,
}

#[derive(Deserialize)]
struct GqlCheckSuite {
    status: Option<String>,
}

#[derive(Deserialize)]
struct GqlComment {
    author: GqlAuthor,
    body: String,
}

#[derive(Deserialize)]
struct GqlTimelineItem {
    #[serde(default)]
    closer: Option<GqlCloser>,
}

#[derive(Deserialize)]
struct GqlCloser {
    #[serde(default, rename = "__typename")]
    typename: Option<String>,
}

// --- Issue types ---

const ISSUE_FIELDS: &str = "
  number title url
  repository { nameWithOwner }
  author { login }
  createdAt updatedAt
  comments { totalCount }
  labels(first: 20) { nodes { name color } }
";

#[derive(Deserialize)]
struct GqlIssueResponse {
    data: GqlIssueData,
}

#[derive(Deserialize)]
struct GqlIssueData {
    search: GqlIssueSearch,
}

#[derive(Deserialize)]
struct GqlIssueSearch {
    #[serde(rename = "pageInfo")]
    page_info: GqlPageInfo,
    nodes: Vec<GqlIssue>,
}

#[derive(Deserialize)]
struct GqlIssue {
    number: i64,
    title: String,
    url: String,
    author: Option<GqlAuthor>,
    repository: GqlRepo,
    #[serde(default, rename = "createdAt")]
    created_at: String,
    #[serde(default, rename = "updatedAt")]
    updated_at: String,
    #[serde(default)]
    comments: GqlNodes<serde_json::Value>,
    #[serde(default)]
    labels: GqlNodes<GqlLabel>,
}

#[derive(Deserialize, Clone)]
struct GqlLabel {
    name: String,
    color: String,
}

fn make_issue_query(search_filter: &str, fields: &str, cursor: Option<&str>) -> String {
    let after = match cursor {
        Some(c) => format!(r#", after: "{}""#, c),
        None => String::new(),
    };
    format!(
        r#"query {{
  search(query: "{}", type: ISSUE, first: 25{}) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      ... on Issue {{
        {}
      }}
    }}
  }}
}}"#,
        search_filter, after, fields
    )
}

async fn run_issue_query(search_filter: &str, fields: &str) -> Result<Vec<GqlIssue>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_nodes = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let query = make_issue_query(search_filter, fields, cursor.as_deref());

        let mut last_err = None;
        let mut resp = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                log!("Retrying issue GraphQL query (attempt {})", attempt + 1);
            }
            let body = match graphql(&query).await {
                Ok(b) => b,
                Err(e) => { last_err = Some(e.to_string()); continue; }
            };

            match serde_json::from_slice::<GqlIssueResponse>(&body) {
                Ok(r) => { resp = Some(r); break; }
                Err(e) => { last_err = Some(format!("JSON parse error: {}", e)); continue; }
            }
        }

        let resp = match resp {
            Some(r) => r,
            None => return Err(format!("Issue GraphQL query failed after 3 attempts: {}", last_err.unwrap_or_default()).into()),
        };

        all_nodes.extend(resp.data.search.nodes);

        if resp.data.search.page_info.has_next_page {
            cursor = resp.data.search.page_info.end_cursor;
        } else {
            break;
        }
    }

    Ok(all_nodes)
}

fn convert_issues(nodes: &[GqlIssue]) -> Vec<IssueInsert> {
    nodes.iter().map(|issue| {
        let labels: Vec<serde_json::Value> = issue.labels.nodes.iter().map(|l| {
            serde_json::json!({"name": l.name, "color": l.color})
        }).collect();

        IssueInsert {
            number: issue.number,
            repo: issue.repository.name_with_owner.clone(),
            title: issue.title.clone(),
            url: issue.url.clone(),
            author: issue.author.as_ref().map(|a| a.login.clone()).unwrap_or_default(),
            created_at: issue.created_at.clone(),
            updated_at: issue.updated_at.clone(),
            comment_count: issue.comments.total_count.unwrap_or(0),
            labels: serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string()),
        }
    }).collect()
}

use crate::db::{MergedPrRow, IssueInsert};

pub struct FetchResult {
    pub my_prs: Vec<PrInsert>,
    pub review_prs: Vec<PrInsert>,
    pub merged_prs: Vec<MergedPrRow>,
    pub issues: Vec<IssueInsert>,
    /// Whether the open-issue query actually succeeded this cycle. False means
    /// the fetch errored (issues is empty as a fallback), so the worker must NOT
    /// reconcile deletions — absence isn't affirmative when the fetch failed.
    pub issues_ok: bool,
    /// (repo, number) of ALL authored PRs that closed in the last 7 days —
    /// including manually closed (not just merged/landed). Used to remove them
    /// from the prs table. merged_prs is a subset used for the display list.
    pub closed_authored: Vec<(String, i64)>,
    /// (repo, number) of PRs the user reviewed (but didn't author) that were
    /// closed in the last 7 days. Used to remove them from review_prs.
    pub closed_reviewed: Vec<(String, i64)>,
}

fn make_query(search_filter: &str, fields: &str, cursor: Option<&str>) -> String {
    let after = match cursor {
        Some(c) => format!(r#", after: "{}""#, c),
        None => String::new(),
    };
    format!(
        r#"query {{
  search(query: "{}", type: ISSUE, first: 25{}) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      ... on PullRequest {{
        {}
      }}
    }}
  }}
}}"#,
        search_filter, after, fields
    )
}

async fn run_query(search_filter: &str, fields: &str) -> Result<Vec<GqlPr>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_nodes = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let query = make_query(search_filter, fields, cursor.as_deref());

        let mut last_err = None;
        let mut resp = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                log!("Retrying GraphQL query (attempt {})", attempt + 1);
            }
            let body = match graphql(&query).await {
                Ok(b) => b,
                Err(e) => { last_err = Some(e.to_string()); continue; }
            };

            match serde_json::from_slice::<GqlResponse>(&body) {
                Ok(r) => { resp = Some(r); break; }
                Err(e) => { last_err = Some(format!("JSON parse error: {}", e)); continue; }
            }
        }

        let resp = match resp {
            Some(r) => r,
            None => return Err(format!("GraphQL query failed after 3 attempts: {}", last_err.unwrap_or_default()).into()),
        };

        all_nodes.extend(resp.data.search.nodes);

        if resp.data.search.page_info.has_next_page {
            cursor = resp.data.search.page_info.end_cursor;
        } else {
            break;
        }
    }

    Ok(all_nodes)
}

fn convert_prs(nodes: &[GqlPr], user: &str) -> Vec<PrInsert> {
    nodes.iter().map(|pr| {
        let (mut review_status, reviewers) = extract_reviews(pr);
        // If you requested changes but the author has since pushed a new commit
        // without re-requesting review, your CHANGES_REQUESTED is stale. Present
        // the decision as undecided so the PR drops out of the changes-requested
        // filter and shows as a normal pending review again. (Your actual review
        // state is still visible in the per-reviewer breakdown / reviewers list.)
        if extract_new_commit_since_review(pr, user) {
            review_status.clear();
        }
        let (drci_emoji, drci_status) = extract_drci(pr);
        let comment_count = extract_comment_count(pr);

        PrInsert {
            number: pr.number,
            repo: pr.repository.name_with_owner.clone(),
            title: pr.title.clone(),
            url: pr.url.clone(),
            state: pr.state.clone(),
            created_at: pr.created_at.clone(),
            updated_at: pr.updated_at.clone(),
            author: pr.author.as_ref().map(|a| a.login.clone()).unwrap_or_default(),
            is_draft: pr.is_draft,
            head_ref_name: pr.head_ref_name.clone(),
            base_ref_name: pr.base_ref_name.clone(),
            review_status,
            reviewers,
            checks_overall: extract_checks_overall(pr),
            checks_running: extract_checks_running(pr),
            drci_status,
            drci_emoji,
            comment_count,
            head_sha: extract_head_sha(pr),
            base_sha: extract_base_sha(pr),
            ci_approval_needed: false,
            re_review_requested: extract_re_review_requested(pr, user),
            drci_ai_verdict: extract_ai_verdict(pr),
            additions: pr.additions,
            deletions: pr.deletions,
        }
    }).collect()
}

/// True when YOU requested changes and the author has since pushed a new commit
/// without re-requesting review. GitHub leaves reviewDecision at
/// CHANGES_REQUESTED in that case, so the PR would otherwise stay hidden behind
/// the changes-requested filter even though the ball is back in your court.
///
/// Detected by SHA, not timestamps: your latest decisive review (APPROVED or
/// CHANGES_REQUESTED, ignoring COMMENTED) is CHANGES_REQUESTED and was made
/// against a commit other than the current head.
fn extract_new_commit_since_review(pr: &GqlPr, user: &str) -> bool {
    if user.is_empty() {
        return false;
    }
    let head = extract_head_sha(pr);
    if head.is_empty() {
        return false;
    }
    // Walk reviews in chronological order; keep my last decisive one.
    let mut my_last: Option<&GqlReview> = None;
    for r in &pr.reviews.nodes {
        if !r.author.login.eq_ignore_ascii_case(user) {
            continue;
        }
        if matches!(r.state.as_str(), "APPROVED" | "CHANGES_REQUESTED") {
            my_last = Some(r);
        }
    }
    match my_last {
        Some(r) if r.state == "CHANGES_REQUESTED" => r
            .commit
            .as_ref()
            .and_then(|c| c.oid.as_deref())
            .is_some_and(|oid| oid != head),
        _ => false,
    }
}

/// True if `user`'s review was *re*-requested: they're in the PR's pending
/// review-request list AND they have already submitted a review on it. GitHub
/// keeps a requested reviewer here until they submit a fresh review, even when
/// their previous review still drives reviewDecision (e.g. a stale
/// CHANGES_REQUESTED). Requiring a prior review distinguishes that from a
/// first-time request (which isn't a re-review and shouldn't bypass the
/// changes-requested hide).
fn extract_re_review_requested(pr: &GqlPr, user: &str) -> bool {
    if user.is_empty() {
        return false;
    }
    let requested = pr.review_requests.nodes.iter().any(|req| {
        req.requested_reviewer
            .as_ref()
            .and_then(|r| r.login.as_deref())
            .is_some_and(|login| login.eq_ignore_ascii_case(user))
    });
    if !requested {
        return false;
    }
    // Only a re-review if you've actually reviewed before (any submitted review;
    // a not-yet-submitted PENDING one doesn't count).
    pr.reviews.nodes.iter().any(|r| {
        r.author.login.eq_ignore_ascii_case(user) && r.state != "PENDING"
    })
}

/// Check a single PR's state via REST. Returns "open", "closed", or "unknown".
pub async fn check_pr_state(repo: &str, number: i64) -> Result<String, BoxErr> {
    let endpoint = format!("repos/{}/pulls/{}", repo, number);
    let body = rest_get(&endpoint).await?;
    let v: serde_json::Value = serde_json::from_slice(&body)?;
    Ok(v.get("state").and_then(|s| s.as_str()).unwrap_or("unknown").to_string())
}

/// Check a single issue's state via REST. Returns "open", "closed", or "unknown".
pub async fn check_issue_state(repo: &str, number: i64) -> Result<String, BoxErr> {
    let endpoint = format!("repos/{}/issues/{}", repo, number);
    let body = rest_get(&endpoint).await?;
    let v: serde_json::Value = serde_json::from_slice(&body)?;
    Ok(v.get("state").and_then(|s| s.as_str()).unwrap_or("unknown").to_string())
}

/// A review PR's authoritative (non-search) relationship to `user`: its state and
/// whether the user is *currently* a requested reviewer or has left a review.
pub struct ReviewRelevance {
    /// "open" / "closed" / "merged" (lowercased).
    pub state: String,
    pub still_requested: bool,
    pub has_review: bool,
}

/// Query a PR directly (not via search, so immune to search-index lag) for
/// whether `user` still has a review relationship with it. Used to evict review
/// PRs the user was un-requested from without ever reviewing — those silently
/// drop out of both search queries but stay OPEN, so a state check alone keeps
/// them forever.
pub async fn check_review_relevance(repo: &str, number: i64, user: &str) -> Result<ReviewRelevance, BoxErr> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid repo format: {}", repo).into());
    }
    let (owner, name) = (parts[0], parts[1]);
    let query = format!(
        r#"query {{
  repository(owner: "{}", name: "{}") {{
    pullRequest(number: {}) {{
      state
      reviewRequests(first: 50) {{ nodes {{ requestedReviewer {{ ... on User {{ login }} }} }} }}
      reviews(first: 100) {{ nodes {{ author {{ login }} }} }}
    }}
  }}
}}"#,
        owner, name, number
    );
    let body = graphql(&query).await?;
    let v: serde_json::Value = serde_json::from_slice(&body)?;
    let pr = &v["data"]["repository"]["pullRequest"];
    if pr.is_null() {
        return Err(format!("PR {}#{} not found", repo, number).into());
    }
    let state = pr["state"].as_str().unwrap_or("unknown").to_lowercase();
    let still_requested = pr["reviewRequests"]["nodes"].as_array().is_some_and(|ns| {
        ns.iter().any(|n| {
            n["requestedReviewer"]["login"].as_str().is_some_and(|l| l.eq_ignore_ascii_case(user))
        })
    });
    let has_review = pr["reviews"]["nodes"].as_array().is_some_and(|ns| {
        ns.iter().any(|n| {
            n["author"]["login"].as_str().is_some_and(|l| l.eq_ignore_ascii_case(user))
        })
    });
    Ok(ReviewRelevance { state, still_requested, has_review })
}

pub async fn check_ci_approval_needed(repo: &str, head_sha: &str) -> bool {
    if head_sha.is_empty() {
        return false;
    }
    let endpoint = format!("repos/{}/actions/runs?head_sha={}&status=action_required&per_page=1", repo, head_sha);
    match rest_get(&endpoint).await {
        Ok(body) => {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            v.get("total_count").and_then(|c| c.as_i64()).unwrap_or(0) > 0
        }
        _ => false,
    }
}

pub async fn fetch_all_prs(_user: &str) -> Result<FetchResult, Box<dyn std::error::Error + Send + Sync>> {
    // Always use @me — it's a first-class GitHub search keyword that reliably
    // resolves to the token owner. Literal usernames can differ in edge cases.
    let user_filter = "@me";
    let my_query = format!("is:pr is:open author:{}", user_filter);
    let review_query = format!("is:pr is:open review-requested:{}", user_filter);
    let reviewed_query = format!("is:pr is:open reviewed-by:{} -author:{}", user_filter, user_filter);
    let (my_nodes, review_nodes, reviewed_nodes) = tokio::try_join!(
        run_query(&my_query, MY_PR_FIELDS),
        run_query(&review_query, REVIEW_PR_FIELDS),
        run_query(&reviewed_query, REVIEW_PR_FIELDS),
    )?;

    // Merge review-requested and reviewed-by, deduplicating by (repo, number)
    let mut seen = std::collections::HashSet::new();
    let mut all_review_nodes = Vec::new();
    for node in review_nodes.into_iter().chain(reviewed_nodes.into_iter()) {
        let key = (node.repository.name_with_owner.clone(), node.number);
        if seen.insert(key) {
            all_review_nodes.push(node);
        }
    }

    let review_prs = convert_prs(&all_review_nodes, _user);

    // Fetch landed PRs separately so failure doesn't block open/review PRs
    let seven_days_ago = {
        let now = std::time::SystemTime::now();
        let seven_days = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        let then = now.duration_since(std::time::UNIX_EPOCH).unwrap() - seven_days;
        let secs = then.as_secs();
        let days = secs / 86400;
        // Convert days since epoch to YYYY-MM-DD
        let (y, m, d) = days_to_ymd(days as i64);
        format!("{:04}-{:02}-{:02}", y, m, d)
    };
    let landed_query = format!("is:pr author:{} closed:>{}", user_filter, seven_days_ago);
    let issue_query = format!("is:issue is:open assignee:{}", user_filter);
    let closed_reviewed_query = format!(
        "is:pr reviewed-by:{} -author:{} closed:>{}",
        user_filter, user_filter, seven_days_ago
    );
    let (landed_result, issues, closed_reviewed) = tokio::join!(
        async {
            match run_query(&landed_query, LANDED_PR_FIELDS).await {
                Ok(nodes) => {
                    let merged = convert_landed_prs(&nodes);
                    let all_closed = extract_all_closed(&nodes);
                    (merged, all_closed)
                }
                Err(e) => {
                    log!("Warning: failed to fetch landed PRs: {}", e);
                    (vec![], vec![])
                }
            }
        },
        async {
            match run_issue_query(&issue_query, ISSUE_FIELDS).await {
                Ok(nodes) => Some(convert_issues(&nodes)),
                Err(e) => {
                    log!("Warning: failed to fetch issues: {}", e);
                    None
                }
            }
        },
        async {
            match run_query(&closed_reviewed_query, REVIEW_PR_FIELDS).await {
                Ok(nodes) => nodes.iter()
                    .map(|n| (n.repository.name_with_owner.clone(), n.number))
                    .collect::<Vec<_>>(),
                Err(e) => {
                    log!("Warning: failed to fetch closed reviewed PRs: {}", e);
                    vec![]
                }
            }
        }
    );

    let (merged_prs, closed_authored) = landed_result;
    let issues_ok = issues.is_some();
    let issues = issues.unwrap_or_default();
    Ok(FetchResult {
        my_prs: convert_prs(&my_nodes, _user),
        review_prs,
        merged_prs,
        issues,
        issues_ok,
        closed_authored,
        closed_reviewed,
    })
}

pub fn days_to_ymd(days_since_epoch: i64) -> (i64, i64, i64) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days_since_epoch + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as i64, d as i64)
}

fn extract_all_closed(nodes: &[GqlPr]) -> Vec<(String, i64)> {
    nodes.iter()
        .map(|pr| (pr.repository.name_with_owner.clone(), pr.number))
        .collect()
}

fn convert_landed_prs(nodes: &[GqlPr]) -> Vec<MergedPrRow> {
    nodes.iter().filter_map(|pr| {
        // Native merge: mergedAt is set
        if let Some(merged_at) = &pr.merged_at {
            return Some(MergedPrRow {
                repo: pr.repository.name_with_owner.clone(),
                number: pr.number,
                title: pr.title.clone(),
                url: pr.url.clone(),
                landed_at: merged_at.clone(),
            });
        }

        // Pytorchmergebot workflow: ClosedEvent with closer of type Commit
        if let Some(timeline) = &pr.timeline_items {
            if let Some(item) = timeline.nodes.last() {
                if let Some(closer) = &item.closer {
                    if closer.typename.as_deref() == Some("Commit") {
                        let landed_at = pr.closed_at.clone().unwrap_or_default();
                        return Some(MergedPrRow {
                            repo: pr.repository.name_with_owner.clone(),
                            number: pr.number,
                            title: pr.title.clone(),
                            url: pr.url.clone(),
                            landed_at,
                        });
                    }
                }
            }
        }

        None // Manually closed, not landed
    }).collect()
}

fn extract_reviews(pr: &GqlPr) -> (String, String) {
    let review_status = pr.review_decision.clone().unwrap_or_default();

    let mut reviewer_map = std::collections::HashMap::new();
    for review in &pr.reviews.nodes {
        reviewer_map.insert(review.author.login.clone(), review.state.clone());
    }
    let reviewers_json = serde_json::to_string(
        &reviewer_map.iter().map(|(login, state)| {
            serde_json::json!({"login": login, "state": state})
        }).collect::<Vec<_>>()
    ).unwrap_or_default();

    (review_status, reviewers_json)
}

fn extract_checks_overall(pr: &GqlPr) -> String {
    let commits = match &pr.commits {
        Some(c) => c,
        None => return String::new(),
    };
    let commit = match commits.nodes.first() {
        Some(c) => c,
        None => return String::new(),
    };
    match &commit.commit.status_check_rollup {
        Some(r) => r.state.clone().unwrap_or_default(),
        None => String::new(),
    }
}

fn extract_checks_running(pr: &GqlPr) -> bool {
    let commits = match &pr.commits {
        Some(c) => c,
        None => return false,
    };
    let commit = match commits.nodes.first() {
        Some(c) => c,
        None => return false,
    };
    let suites = match &commit.commit.check_suites {
        Some(s) => s,
        None => return false,
    };
    suites.nodes.iter().any(|s| {
        matches!(s.status.as_deref(), Some("IN_PROGRESS") | Some("WAITING"))
    })
}

fn extract_drci(pr: &GqlPr) -> (String, String) {
    let comments = match &pr.comments {
        Some(c) => &c.nodes,
        None => return (String::new(), String::new()),
    };
    for comment in comments {
        if comment.body.contains("drci-comment-start") {
            for line in comment.body.lines() {
                if line.starts_with("## :") && !line.contains("Helpful Links") && !line.contains("Active SEV") {
                    if let Some(rest) = line.strip_prefix("## :") {
                        if let Some(colon_pos) = rest.find(':') {
                            let mut emoji = rest[..colon_pos].to_string();
                            let status = rest[colon_pos + 1..].trim().to_string();
                            if status.starts_with("You can merge normally") {
                                emoji = "white_check_mark".to_string();
                            }
                            return (emoji, status);
                        }
                    }
                }
            }
            break;
        }
    }
    (String::new(), String::new())
}

fn extract_head_sha(pr: &GqlPr) -> String {
    pr.commits.as_ref()
        .and_then(|c| c.nodes.first())
        .and_then(|n| n.commit.oid.clone())
        .unwrap_or_default()
}

fn extract_base_sha(pr: &GqlPr) -> String {
    pr.base_ref.as_ref()
        .and_then(|r| r.target.as_ref())
        .and_then(|t| t.oid.clone())
        .unwrap_or_default()
}

// --- Per-PR detail query types ---

#[derive(Deserialize)]
struct DetailResponse {
    data: DetailData,
}

#[derive(Deserialize)]
struct DetailData {
    repository: DetailRepo,
}

#[derive(Deserialize)]
struct DetailRepo {
    #[serde(rename = "pullRequest")]
    pull_request: DetailPr,
}

#[derive(Deserialize)]
struct DetailPr {
    commits: GqlNodes<DetailCommitNode>,
    comments: GqlNodes<DetailComment>,
}

#[derive(Deserialize)]
struct DetailCommitNode {
    commit: DetailCommit,
}

#[derive(Deserialize)]
struct DetailCommit {
    #[serde(rename = "committedDate")]
    committed_date: Option<String>,
    #[serde(rename = "statusCheckRollup")]
    status_check_rollup: Option<DetailRollup>,
}

#[derive(Deserialize)]
struct DetailRollup {
    contexts: DetailContexts,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct DetailContexts {
    #[serde(rename = "totalCount")]
    total_count: i64,
    #[serde(rename = "pageInfo")]
    page_info: GqlPageInfo,
    nodes: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct DetailComment {
    author: Option<GqlAuthor>,
    body: String,
    #[serde(rename = "createdAt")]
    created_at: Option<String>,
    #[serde(default, rename = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Deserialize)]
struct ContextPageResponse {
    data: ContextPageData,
}

#[derive(Deserialize)]
struct ContextPageData {
    repository: ContextPageRepo,
}

#[derive(Deserialize)]
struct ContextPageRepo {
    #[serde(rename = "pullRequest")]
    pull_request: ContextPagePr,
}

#[derive(Deserialize)]
struct ContextPagePr {
    commits: GqlNodes<ContextPageCommitNode>,
}

#[derive(Deserialize)]
struct ContextPageCommitNode {
    commit: ContextPageCommit,
}

#[derive(Deserialize)]
struct ContextPageCommit {
    #[serde(rename = "statusCheckRollup")]
    status_check_rollup: Option<ContextPageRollup>,
}

#[derive(Deserialize)]
struct ContextPageRollup {
    contexts: ContextPage,
}

#[derive(Deserialize)]
struct ContextPage {
    #[serde(rename = "pageInfo")]
    page_info: GqlPageInfo,
    nodes: Vec<serde_json::Value>,
}

use crate::db::PrDetailUpdate;

pub async fn fetch_pr_details(repo: &str, number: i64, include_landing: bool, mention_user: &str) -> Result<PrDetailUpdate, Box<dyn std::error::Error + Send + Sync>> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid repo format: {}", repo).into());
    }
    let (owner, name) = (parts[0], parts[1]);

    // First query: comments + first page of check contexts
    let query = format!(
        r#"query {{
  repository(owner: "{}", name: "{}") {{
    pullRequest(number: {}) {{
      commits(last:1) {{ nodes {{ commit {{
        committedDate
        statusCheckRollup {{
          contexts(first:100) {{ totalCount pageInfo {{ hasNextPage endCursor }} nodes {{
            ... on CheckRun {{ status conclusion }}
            ... on StatusContext {{ state }}
          }} }}
        }}
      }} }} }}
      comments(last:100) {{ nodes {{ author {{ login }} body createdAt updatedAt }} }}
    }}
  }}
}}"#,
        owner, name, number
    );

    let body = graphql(&query).await?;

    let resp: DetailResponse = serde_json::from_slice(&body)?;
    let pr = &resp.data.repository.pull_request;

    // Collect all context nodes (paginate if needed)
    let mut all_context_nodes = Vec::new();
    let mut committed_date = None;

    if let Some(commit_node) = pr.commits.nodes.first() {
        committed_date = commit_node.commit.committed_date.clone();
        if let Some(rollup) = &commit_node.commit.status_check_rollup {
            all_context_nodes.extend(rollup.contexts.nodes.iter().cloned());

            // Paginate remaining contexts
            let mut has_next = rollup.contexts.page_info.has_next_page;
            let mut cursor = rollup.contexts.page_info.end_cursor.clone();

            while has_next {
                let after = cursor.as_deref().unwrap_or("");
                let page_query = format!(
                    r#"query {{
  repository(owner: "{}", name: "{}") {{
    pullRequest(number: {}) {{
      commits(last:1) {{ nodes {{ commit {{ statusCheckRollup {{
        contexts(first:100, after: "{}") {{ pageInfo {{ hasNextPage endCursor }} nodes {{
          ... on CheckRun {{ status conclusion }}
          ... on StatusContext {{ state }}
        }} }}
      }} }} }} }}
    }}
  }}
}}"#,
                    owner, name, number, after
                );

                let page_body = match graphql(&page_query).await {
                    Ok(b) => b,
                    Err(_) => break,
                };

                let page_resp: ContextPageResponse = serde_json::from_slice(&page_body)?;
                if let Some(cn) = page_resp.data.repository.pull_request.commits.nodes.first() {
                    if let Some(r) = &cn.commit.status_check_rollup {
                        all_context_nodes.extend(r.contexts.nodes.iter().cloned());
                        has_next = r.contexts.page_info.has_next_page;
                        cursor = r.contexts.page_info.end_cursor.clone();
                        continue;
                    }
                }
                break;
            }
        }
    }

    let (success, fail, pending, queued) = count_check_contexts(&all_context_nodes);

    // Extract DrCI from comments
    let (drci_emoji, drci_status, drci_updated_at) = extract_drci_from_detail_comments(&pr.comments.nodes);

    // Extract landing status from pytorchmergebot comments
    let landing_status = if include_landing {
        extract_landing_status(&pr.comments.nodes, committed_date.as_deref())
    } else {
        String::new()
    };

    let mention_count = count_mentions(&pr.comments.nodes, mention_user);

    Ok(PrDetailUpdate {
        checks_success: success,
        checks_fail: fail,
        checks_pending: pending,
        checks_queued: queued,
        checks_running: pending > 0,
        drci_emoji,
        drci_status,
        drci_updated_at,
        drci_ai_verdict: extract_ai_verdict_from_detail_comments(&pr.comments.nodes),
        landing_status,
        mention_count,
        comment_count: count_human_comments(&pr.comments.nodes),
    })
}

/// Whether a comment author is a bot whose comments shouldn't re-surface a PR.
/// Covers GitHub Apps (login ends in "[bot]") plus the pytorch/meta service
/// accounts and the `github-actions` stale bot (which posts the "Stale" comment).
fn is_bot_comment(login: &str) -> bool {
    login.ends_with("[bot]")
        || matches!(
            login,
            "pytorch-bot" | "pytorchmergebot" | "facebook-github-bot" | "github-actions"
        )
}

/// Count only human (non-bot) comments. Used as the review-PR comment signal for
/// auto-unread so bot chatter (DrCI refreshes, the "Stale" bot, merge bot) can't
/// pop a PR you've already triaged back to unread.
fn count_human_comments(comments: &[DetailComment]) -> i64 {
    comments.iter()
        .filter(|c| {
            let login = c.author.as_ref().map(|a| a.login.as_str()).unwrap_or("");
            !is_bot_comment(login)
        })
        .count() as i64
}

/// Count comments that mention `@<user>`. Word-boundary aware so `@aorenste`
/// doesn't match `@aorensteward`. Returns 0 if user is empty.
fn count_mentions(comments: &[DetailComment], user: &str) -> i64 {
    if user.is_empty() {
        return 0;
    }
    let needle = format!("@{}", user.to_lowercase());
    let mut count: i64 = 0;
    for c in comments {
        let body = c.body.to_lowercase();
        let mut idx = 0;
        let mut found = false;
        while let Some(pos) = body[idx..].find(&needle) {
            let abs = idx + pos;
            let after = abs + needle.len();
            let is_boundary = body.as_bytes().get(after).map_or(true, |&b| {
                let ch = b as char;
                !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
            });
            if is_boundary {
                found = true;
                break;
            }
            idx = abs + needle.len();
        }
        if found {
            count += 1;
        }
    }
    count
}

/// Returns (emoji, status, drci_updated_at). The timestamp is the DrCI comment's
/// last edit time (it refreshes every ~15 min) and is returned whenever the DrCI
/// comment exists, even if it hasn't posted a status line yet — that lets callers
/// tell "DrCI is still catching up" from "DrCI is stuck".
fn extract_drci_from_detail_comments(comments: &[DetailComment]) -> (String, String, String) {
    for comment in comments {
        let login = comment.author.as_ref().map(|a| a.login.as_str()).unwrap_or("");
        if login != "pytorch-bot" && login != "pytorch-bot[bot]" {
            continue;
        }
        if !comment.body.contains("drci-comment-start") {
            continue;
        }
        let updated_at = comment.updated_at.clone()
            .or_else(|| comment.created_at.clone())
            .unwrap_or_default();
        for line in comment.body.lines() {
            if line.starts_with("## :") && !line.contains("Helpful Links") && !line.contains("Active SEV") {
                if let Some(rest) = line.strip_prefix("## :") {
                    if let Some(colon_pos) = rest.find(':') {
                        let mut emoji = rest[..colon_pos].to_string();
                        let status = rest[colon_pos + 1..].trim().to_string();
                        if status.starts_with("You can merge normally") {
                            emoji = "white_check_mark".to_string();
                        }
                        return (emoji, status, updated_at);
                    }
                }
            }
        }
        // DrCI comment exists but has no status verdict yet — still report when
        // it was last touched so staleness can be judged.
        return (String::new(), String::new(), updated_at);
    }
    (String::new(), String::new(), String::new())
}

/// Buckets of per-failure AI advisor verdicts found in a DrCI comment. DrCI's
/// summary emoji ignores these, so a PR can show a red `:x:` while the AI has
/// judged every failure it looked at as "not related" (safe to merge).
/// test-infra emits a machine-readable `alt="AI verdict: <label>"` for each
/// new/unclassified failure (see advisorBadge.ts) — exactly the failures that
/// drive the red `:x:`. We tally those labels.
#[derive(Default, PartialEq, Debug, Clone, Copy)]
struct AiVerdicts {
    /// AI thinks the failure is related to this PR (blocking): "related",
    /// "probably related".
    related: i64,
    /// AI thinks the failure isn't the PR's fault (safe): "not related",
    /// "probably not related", plus suppressed signals ("garbage"/"infra issue").
    not_related: i64,
    /// AI couldn't decide confidently: "inconclusive" or any "(uncertain)".
    uncertain: i64,
    /// Advisor still analyzing (the "pending" sentinel, badge shows "analyzing").
    pending: i64,
}

/// Tally the `alt="AI verdict: <label>"` badges in a DrCI comment body.
fn parse_ai_verdicts(body: &str) -> AiVerdicts {
    let needle = "alt=\"AI verdict: ";
    let mut v = AiVerdicts::default();
    let mut rest = body;
    while let Some(pos) = rest.find(needle) {
        let after = &rest[pos + needle.len()..];
        let end = match after.find('"') {
            Some(e) => e,
            None => break,
        };
        let label = after[..end].to_ascii_lowercase();
        rest = &after[end..];
        // Order matters: the less-confident / more-specific labels are checked
        // first so "not related" isn't swallowed by the bare "related" arm and
        // any "(uncertain)" verdict is bucketed conservatively, not as a pole.
        if label.contains("pending") || label.contains("analyzing") {
            v.pending += 1;
        } else if label.contains("uncertain") || label.contains("inconclusive") {
            v.uncertain += 1;
        } else if label.contains("not related") {
            v.not_related += 1;
        } else if label.contains("related") {
            v.related += 1;
        } else if label.contains("garbage") || label.contains("infra") {
            v.not_related += 1;
        } else {
            // Unknown/blank label: don't guess a pole.
            v.uncertain += 1;
        }
    }
    v
}

/// Serialize AI verdict tallies to the compact JSON blob we store. Returns ""
/// when there are none, so the light query (which never sees comments) and the
/// "advisor not enabled" case store the same empty sentinel.
fn ai_verdict_json(body: &str) -> String {
    let v = parse_ai_verdicts(body);
    if v == AiVerdicts::default() {
        return String::new();
    }
    serde_json::json!({
        "related": v.related,
        "not_related": v.not_related,
        "uncertain": v.uncertain,
        "pending": v.pending,
    })
    .to_string()
}

/// Find the DrCI comment among a PR's list-level comments and tally its AI
/// verdict badges. Empty when there's no DrCI comment or it has no verdicts.
fn extract_ai_verdict(pr: &GqlPr) -> String {
    let comments = match &pr.comments {
        Some(c) => &c.nodes,
        None => return String::new(),
    };
    for comment in comments {
        if comment.body.contains("drci-comment-start") {
            return ai_verdict_json(&comment.body);
        }
    }
    String::new()
}

/// Same as `extract_ai_verdict` but for the richer detail-fetch comments, which
/// carry author info so we can require the pytorch-bot DrCI comment specifically.
fn extract_ai_verdict_from_detail_comments(comments: &[DetailComment]) -> String {
    for comment in comments {
        let login = comment.author.as_ref().map(|a| a.login.as_str()).unwrap_or("");
        if login != "pytorch-bot" && login != "pytorch-bot[bot]" {
            continue;
        }
        if comment.body.contains("drci-comment-start") {
            return ai_verdict_json(&comment.body);
        }
    }
    String::new()
}

fn extract_landing_status(comments: &[DetailComment], committed_date: Option<&str>) -> String {
    // Find the last pytorchmergebot comment with landing info
    let mut last_status = String::new();
    let mut last_created_at = String::new();

    for comment in comments {
        let login = comment.author.as_ref().map(|a| a.login.as_str()).unwrap_or("");
        if login != "pytorchmergebot" && login != "pytorchmergebot[bot]" {
            continue;
        }

        if comment.body.contains("### Merge started") {
            last_status = "landing".to_string();
            last_created_at = comment.created_at.clone().unwrap_or_default();
        } else if comment.body.contains("successfully reverted")
            || comment.body.contains("has been successfully reverted")
        {
            last_status = "reverted".to_string();
            last_created_at = comment.created_at.clone().unwrap_or_default();
        } else if comment.body.contains("failed to merge") || comment.body.contains("Merge failed")
        {
            last_status = "failed".to_string();
            last_created_at = comment.created_at.clone().unwrap_or_default();
        }
    }

    // Reset rule: if head commit is newer than the last landing comment, clear status
    if !last_status.is_empty() && last_status != "landing" {
        if let (Some(commit_date), true) = (committed_date, !last_created_at.is_empty()) {
            // Both are ISO 8601 UTC strings from GitHub, lexicographic comparison works
            if commit_date > last_created_at.as_str() {
                return String::new();
            }
        }
    }

    last_status
}

fn extract_comment_count(pr: &GqlPr) -> i64 {
    match &pr.comments {
        Some(c) if !c.nodes.is_empty() => c.nodes.iter()
            .filter(|c| !matches!(c.author.login.as_str(), "pytorch-bot" | "facebook-github-bot"))
            .count() as i64,
        Some(c) => c.total_count.unwrap_or(0),
        None => 0,
    }
}

fn count_check_contexts(nodes: &[serde_json::Value]) -> (i64, i64, i64, i64) {
    // queued is a subset of pending: jobs that haven't started running yet.
    let (mut success, mut fail, mut pending, mut queued) = (0i64, 0i64, 0i64, 0i64);
    for node in nodes {
        if let Some(conclusion) = node.get("conclusion") {
            if conclusion.is_null() {
                pending += 1;
                let status = node.get("status").and_then(|s| s.as_str()).unwrap_or("");
                if matches!(status, "QUEUED" | "WAITING" | "REQUESTED" | "PENDING") {
                    queued += 1;
                }
            } else {
                match conclusion.as_str().unwrap_or("") {
                    "SUCCESS" | "NEUTRAL" | "SKIPPED" => success += 1,
                    "FAILURE" | "TIMED_OUT" | "CANCELLED" | "ACTION_REQUIRED" => fail += 1,
                    _ => pending += 1,
                }
            }
        } else if let Some(state) = node.get("state").and_then(|s| s.as_str()) {
            // StatusContext has no queued/running distinction, so all non-terminal
            // states are counted as pending only.
            match state {
                "SUCCESS" => success += 1,
                "FAILURE" | "ERROR" => fail += 1,
                _ => pending += 1,
            }
        }
    }
    (success, fail, pending, queued)
}

pub const DETAIL_BATCH_SIZE: usize = 8;

const DETAIL_PR_FRAGMENT: &str = "
    commits(last:1) { nodes { commit {
      committedDate
      statusCheckRollup {
        contexts(first:100) { totalCount pageInfo { hasNextPage endCursor } nodes {
          ... on CheckRun { status conclusion }
          ... on StatusContext { state }
        } }
      }
    } } }
    comments(last:100) { nodes { author { login } body createdAt updatedAt } }
";

/// Fetch details for multiple PRs in the same repo using a single aliased GraphQL query.
/// Returns results for each PR that parsed successfully, plus a list of PR numbers
/// that need individual follow-up (context pagination >100 checks).
pub async fn fetch_pr_details_batch(
    repo: &str,
    prs: &[(i64, bool)], // (number, include_landing)
    mention_user: &str,
) -> Result<(Vec<(i64, PrDetailUpdate)>, Vec<(i64, bool)>), BoxErr> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid repo format: {}", repo).into());
    }
    let (owner, name) = (parts[0], parts[1]);

    // Build aliased query
    let aliases: Vec<String> = prs.iter().map(|(num, _)| {
        format!("    pr_{}: pullRequest(number: {}) {{{}}}", num, num, DETAIL_PR_FRAGMENT)
    }).collect();

    let query = format!(
        "query {{\n  repository(owner: \"{}\", name: \"{}\") {{\n{}\n  }}\n}}",
        owner, name, aliases.join("\n")
    );

    let body = graphql(&query).await?;
    let root: serde_json::Value = serde_json::from_slice(&body)?;

    let repo_obj = root
        .get("data")
        .and_then(|d| d.get("repository"))
        .and_then(|r| r.as_object());

    let repo_obj = match repo_obj {
        Some(obj) => obj,
        None => return Ok((vec![], vec![])),
    };

    let include_map: std::collections::HashMap<i64, bool> = prs.iter().copied().collect();
    let mut results = Vec::new();
    let mut needs_pagination = Vec::new();

    for (key, val) in repo_obj {
        let num: i64 = match key.strip_prefix("pr_").and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => continue,
        };
        if val.is_null() {
            continue; // PR deleted or inaccessible
        }

        let pr: DetailPr = match serde_json::from_value(val.clone()) {
            Ok(p) => p,
            Err(e) => {
                log!("Warning: failed to parse detail for {}#{}: {}", repo, num, e);
                continue;
            }
        };

        let mut all_context_nodes = Vec::new();
        let mut committed_date = None;
        let mut has_more_contexts = false;

        if let Some(commit_node) = pr.commits.nodes.first() {
            committed_date = commit_node.commit.committed_date.clone();
            if let Some(rollup) = &commit_node.commit.status_check_rollup {
                all_context_nodes.extend(rollup.contexts.nodes.iter().cloned());
                if rollup.contexts.page_info.has_next_page {
                    has_more_contexts = true;
                }
            }
        }

        if has_more_contexts {
            let include_landing = include_map.get(&num).copied().unwrap_or(false);
            needs_pagination.push((num, include_landing));
            continue;
        }

        let (success, fail, pending, queued) = count_check_contexts(&all_context_nodes);
        let (drci_emoji, drci_status, drci_updated_at) = extract_drci_from_detail_comments(&pr.comments.nodes);
        let include_landing = include_map.get(&num).copied().unwrap_or(false);
        let landing_status = if include_landing {
            extract_landing_status(&pr.comments.nodes, committed_date.as_deref())
        } else {
            String::new()
        };
        let mention_count = count_mentions(&pr.comments.nodes, mention_user);

        results.push((num, PrDetailUpdate {
            checks_success: success,
            checks_fail: fail,
            checks_pending: pending,
            checks_queued: queued,
            checks_running: pending > 0,
            drci_emoji,
            drci_status,
            drci_updated_at,
            drci_ai_verdict: extract_ai_verdict_from_detail_comments(&pr.comments.nodes),
            landing_status,
            mention_count,
            comment_count: count_human_comments(&pr.comments.nodes),
        }));
    }

    Ok((results, needs_pagination))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(body: &str) -> DetailComment {
        DetailComment {
            author: None,
            body: body.to_string(),
            created_at: None,
            updated_at: None,
        }
    }

    fn authored(login: &str, body: &str) -> DetailComment {
        DetailComment {
            author: Some(GqlAuthor { login: login.to_string() }),
            body: body.to_string(),
            created_at: None,
            updated_at: None,
        }
    }

    #[test]
    fn is_bot_comment_detects_bots_and_apps() {
        for b in ["github-actions", "github-actions[bot]", "pytorch-bot",
                  "pytorchmergebot", "facebook-github-bot", "dependabot[bot]"] {
            assert!(is_bot_comment(b), "{} should be a bot", b);
        }
        for h in ["jansel", "aorenste", "williamsnell"] {
            assert!(!is_bot_comment(h), "{} should be human", h);
        }
    }

    #[test]
    fn count_human_comments_excludes_bots() {
        // Repro for #186458: the github-actions "Stale" comment (and other bot
        // chatter) must not count toward the auto-unread comment signal.
        let cs = vec![
            authored("jansel", "please fix"),
            authored("github-actions", "Looks like this PR hasn't been updated in a while..."),
            authored("pytorch-bot", "<!-- ciflow-pending -->"),
            authored("williamsnell", "lgtm"),
        ];
        assert_eq!(count_human_comments(&cs), 2);
    }

    fn drci_comment(body: &str, updated_at: Option<&str>) -> DetailComment {
        DetailComment {
            author: Some(GqlAuthor { login: "pytorch-bot".to_string() }),
            body: format!("<!-- drci-comment-start -->\n{}", body),
            created_at: Some("2026-01-01T00:00:00Z".to_string()),
            updated_at: updated_at.map(|s| s.to_string()),
        }
    }

    #[test]
    fn drci_verdict_returns_emoji_status_and_timestamp() {
        let cs = vec![drci_comment(
            "## :white_check_mark: All checks passing",
            Some("2026-06-01T00:14:34Z"),
        )];
        let (emoji, status, updated_at) = extract_drci_from_detail_comments(&cs);
        assert_eq!(emoji, "white_check_mark");
        assert_eq!(status, "All checks passing");
        assert_eq!(updated_at, "2026-06-01T00:14:34Z");
    }

    #[test]
    fn drci_merge_normally_normalizes_to_check_mark() {
        let cs = vec![drci_comment(
            "## :hourglass: You can merge normally now",
            Some("2026-06-01T00:14:34Z"),
        )];
        let (emoji, _status, _) = extract_drci_from_detail_comments(&cs);
        assert_eq!(emoji, "white_check_mark");
    }

    #[test]
    fn drci_comment_without_verdict_still_returns_timestamp() {
        // Repro for #184133: DrCI has only posted the Helpful Links section, no
        // status line yet. We must still surface when it last refreshed so the
        // frontend can judge staleness.
        let cs = vec![drci_comment(
            "## :link: Helpful Links\n* some link",
            Some("2026-06-01T00:14:34Z"),
        )];
        let (emoji, status, updated_at) = extract_drci_from_detail_comments(&cs);
        assert_eq!(emoji, "");
        assert_eq!(status, "");
        assert_eq!(updated_at, "2026-06-01T00:14:34Z");
    }

    #[test]
    fn drci_timestamp_falls_back_to_created_at() {
        let mut c = drci_comment("## :link: Helpful Links", None);
        c.created_at = Some("2026-05-01T00:00:00Z".to_string());
        let (_emoji, _status, updated_at) = extract_drci_from_detail_comments(&[c]);
        assert_eq!(updated_at, "2026-05-01T00:00:00Z");
    }

    #[test]
    fn no_drci_comment_returns_empty_timestamp() {
        let cs = vec![comment("just a regular comment")];
        let (_emoji, _status, updated_at) = extract_drci_from_detail_comments(&cs);
        assert_eq!(updated_at, "");
    }

    // A DrCI comment body with `n` AI verdict badges, one per supplied label.
    // Mirrors the `alt="AI verdict: <label>"` markup emitted by test-infra's
    // advisorBadge.ts renderVerdictLine/renderInProgressLine.
    fn body_with_verdicts(labels: &[&str]) -> String {
        let mut s = String::from("## :x: 1 Unclassified Failure\n");
        for label in labels {
            s.push_str(&format!(
                "  <details><summary>AI verdict: <a href=\"h\"><img alt=\"AI verdict: {}\" src=\"s\"></a></summary></details>\n",
                label
            ));
        }
        s
    }

    #[test]
    fn ai_verdict_buckets_each_label() {
        let v = parse_ai_verdicts(&body_with_verdicts(&[
            "related",
            "probably related",
            "not related",
            "probably not related",
            "not related (uncertain)",
            "related (uncertain)",
            "inconclusive",
            "garbage",
            "infra issue",
            "pending",
        ]));
        assert_eq!(
            v,
            AiVerdicts {
                // related + probably related
                related: 2,
                // not related + probably not related + garbage + infra issue
                not_related: 4,
                // not related (uncertain) + related (uncertain) + inconclusive
                uncertain: 3,
                pending: 1,
            }
        );
    }

    #[test]
    fn ai_verdict_matches_real_drci_markup() {
        // The exact "not related" line shape from pytorch/pytorch#189486.
        let body = concat!(
            "## :x: 5 Pending, 1 Unrelated Failure, 1 Unclassified Failure\n",
            "  <details><summary>AI verdict: ",
            "<a href=\"https://hud.pytorch.org/pr/pytorch/pytorch/189486#86229846571\">",
            "<img alt=\"AI verdict: not related\" ",
            "src=\"https://hud.pytorch.org/api/drci/advisorBadge?owner=pytorch\"></a>",
            "</summary><blockquote>\n</blockquote></details>\n",
        );
        assert_eq!(ai_verdict_json(body),
            r#"{"not_related":1,"pending":0,"related":0,"uncertain":0}"#);
    }

    #[test]
    fn ai_verdict_json_empty_when_no_badges() {
        assert_eq!(ai_verdict_json("## :x: 1 New Failure\nno advisor here"), "");
    }

    #[test]
    fn extract_ai_verdict_finds_pytorch_bot_drci_comment() {
        let cs = vec![
            comment("a human commented: AI verdict: related (should be ignored)"),
            drci_comment(&body_with_verdicts(&["not related", "not related"]), None),
        ];
        assert_eq!(extract_ai_verdict_from_detail_comments(&cs),
            r#"{"not_related":2,"pending":0,"related":0,"uncertain":0}"#);
    }

    #[test]
    fn extract_ai_verdict_ignores_non_drci_comments() {
        // A non-pytorch-bot comment mentioning the phrase must not be counted.
        let cs = vec![comment(&body_with_verdicts(&["related"]))];
        assert_eq!(extract_ai_verdict_from_detail_comments(&cs), "");
    }

    #[test]
    fn empty_user_returns_zero() {
        let cs = vec![comment("hey @aorenste look at this")];
        assert_eq!(count_mentions(&cs, ""), 0);
    }

    #[test]
    fn no_mentions_returns_zero() {
        let cs = vec![comment("just some prose"), comment("more prose")];
        assert_eq!(count_mentions(&cs, "aorenste"), 0);
    }

    #[test]
    fn single_mention_counts_once() {
        let cs = vec![comment("hey @aorenste look")];
        assert_eq!(count_mentions(&cs, "aorenste"), 1);
    }

    #[test]
    fn multiple_comments_each_count() {
        let cs = vec![
            comment("hey @aorenste"),
            comment("ping @aorenste"),
            comment("nothing here"),
        ];
        assert_eq!(count_mentions(&cs, "aorenste"), 2);
    }

    #[test]
    fn multiple_in_same_comment_counts_once() {
        let cs = vec![comment("@aorenste @aorenste twice in one")];
        assert_eq!(count_mentions(&cs, "aorenste"), 1);
    }

    #[test]
    fn case_insensitive() {
        let cs = vec![comment("hey @Aorenste"), comment("@AORENSTE")];
        assert_eq!(count_mentions(&cs, "aorenste"), 2);
    }

    #[test]
    fn word_boundary_blocks_longer_match() {
        // @aorensteward should not match @aorenste
        let cs = vec![comment("hi @aorensteward")];
        assert_eq!(count_mentions(&cs, "aorenste"), 0);
    }

    #[test]
    fn word_boundary_allows_punctuation() {
        let cs = vec![
            comment("@aorenste, please review"),
            comment("ping @aorenste."),
            comment("see @aorenste\nthanks"),
        ];
        assert_eq!(count_mentions(&cs, "aorenste"), 3);
    }

    #[test]
    fn handle_at_end_of_body() {
        let cs = vec![comment("ping @aorenste")];
        assert_eq!(count_mentions(&cs, "aorenste"), 1);
    }

    #[test]
    fn falls_through_to_real_match_after_partial() {
        // First @aorensteward fails the boundary; the trailing @aorenste should still count.
        let cs = vec![comment("@aorensteward and also @aorenste")];
        assert_eq!(count_mentions(&cs, "aorenste"), 1);
    }

    #[test]
    fn hyphen_and_underscore_are_part_of_handle() {
        let cs = vec![
            comment("@aorenste-ext"),
            comment("@aorenste_other"),
        ];
        assert_eq!(count_mentions(&cs, "aorenste"), 0);
    }

    fn closed_pr(number: i64, merged: bool) -> GqlPr {
        GqlPr {
            number,
            title: format!("PR #{}", number),
            url: format!("https://github.com/pytorch/pytorch/pull/{}", number),
            author: Some(GqlAuthor { login: "aorenste".to_string() }),
            is_draft: false,
            additions: 0,
            deletions: 0,
            head_ref_name: String::new(),
            base_ref_name: String::new(),
            base_ref: None,
            repository: GqlRepo { name_with_owner: "pytorch/pytorch".to_string() },
            state: "CLOSED".to_string(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-02T00:00:00Z".to_string(),
            review_decision: None,
            reviews: Default::default(),
            review_requests: Default::default(),
            commits: None,
            comments: None,
            merged_at: if merged { Some("2025-01-02T00:00:00Z".to_string()) } else { None },
            closed_at: Some("2025-01-02T00:00:00Z".to_string()),
            timeline_items: None,
        }
    }

    fn pr_with_review_requests(logins: &[Option<&str>]) -> GqlPr {
        let mut pr = closed_pr(1, false);
        pr.review_requests = GqlNodes {
            nodes: logins.iter().map(|l| GqlReviewRequest {
                requested_reviewer: Some(GqlRequestedReviewer {
                    login: l.map(|s| s.to_string()),
                }),
            }).collect(),
            total_count: None,
        };
        pr
    }

    // Attach prior reviews (by login) to a PR — used to distinguish a genuine
    // re-review from a first-time review request.
    fn with_reviewers(mut pr: GqlPr, logins: &[&str]) -> GqlPr {
        pr.reviews = GqlNodes {
            nodes: logins.iter().map(|l| GqlReview {
                author: GqlAuthor { login: l.to_string() },
                state: "COMMENTED".to_string(),
                commit: None,
            }).collect(),
            total_count: None,
        };
        pr
    }

    // Build a PR with a head commit `head` and a set of (login, state, reviewed_oid)
    // reviews, for testing extract_new_commit_since_review.
    fn pr_with_reviews(head: &str, reviews: &[(&str, &str, &str)]) -> GqlPr {
        let mut pr = closed_pr(1, false);
        pr.commits = Some(GqlNodes {
            nodes: vec![GqlCommitNode {
                commit: GqlCommit {
                    oid: Some(head.to_string()),
                    status_check_rollup: None,
                    check_suites: None,
                },
            }],
            total_count: None,
        });
        pr.reviews = GqlNodes {
            nodes: reviews.iter().map(|(login, state, oid)| GqlReview {
                author: GqlAuthor { login: login.to_string() },
                state: state.to_string(),
                commit: Some(GqlReviewCommit { oid: Some(oid.to_string()) }),
            }).collect(),
            total_count: None,
        };
        pr
    }

    #[test]
    fn new_commit_when_changes_requested_against_older_commit() {
        let pr = pr_with_reviews("sha2", &[("aorenste", "CHANGES_REQUESTED", "sha1")]);
        assert!(extract_new_commit_since_review(&pr, "aorenste"));
    }

    #[test]
    fn no_new_commit_when_reviewed_current_head() {
        let pr = pr_with_reviews("sha1", &[("aorenste", "CHANGES_REQUESTED", "sha1")]);
        assert!(!extract_new_commit_since_review(&pr, "aorenste"));
    }

    #[test]
    fn no_new_commit_when_i_approved_after_requesting_changes() {
        let pr = pr_with_reviews("sha2", &[
            ("aorenste", "CHANGES_REQUESTED", "sha1"),
            ("aorenste", "APPROVED", "sha2"),
        ]);
        assert!(!extract_new_commit_since_review(&pr, "aorenste"));
    }

    #[test]
    fn comment_after_changes_requested_does_not_reset_decision() {
        // A COMMENTED review isn't decisive — my last decisive review is still
        // CHANGES_REQUESTED, made against an older commit.
        let pr = pr_with_reviews("sha2", &[
            ("aorenste", "CHANGES_REQUESTED", "sha1"),
            ("aorenste", "COMMENTED", "sha1"),
        ]);
        assert!(extract_new_commit_since_review(&pr, "aorenste"));
    }

    #[test]
    fn another_reviewers_changes_request_ignored() {
        let pr = pr_with_reviews("sha2", &[("zou3519", "CHANGES_REQUESTED", "sha1")]);
        assert!(!extract_new_commit_since_review(&pr, "aorenste"));
    }

    #[test]
    fn new_commit_since_review_empty_user_false() {
        let pr = pr_with_reviews("sha2", &[("aorenste", "CHANGES_REQUESTED", "sha1")]);
        assert!(!extract_new_commit_since_review(&pr, ""));
    }

    #[test]
    fn new_commit_clears_review_status_in_convert_prs() {
        // A stale CHANGES_REQUESTED is presented as undecided so it falls out of
        // the changes-requested filter and shows as a pending review.
        let mut pr = pr_with_reviews("sha2", &[("aorenste", "CHANGES_REQUESTED", "sha1")]);
        pr.review_decision = Some("CHANGES_REQUESTED".to_string());
        let inserts = convert_prs(std::slice::from_ref(&pr), "aorenste");
        assert_eq!(inserts[0].review_status, "");
    }

    #[test]
    fn changes_requested_kept_when_no_new_commit() {
        // Same head as the review -> not stale -> keep CHANGES_REQUESTED.
        let mut pr = pr_with_reviews("sha1", &[("aorenste", "CHANGES_REQUESTED", "sha1")]);
        pr.review_decision = Some("CHANGES_REQUESTED".to_string());
        let inserts = convert_prs(std::slice::from_ref(&pr), "aorenste");
        assert_eq!(inserts[0].review_status, "CHANGES_REQUESTED");
    }

    #[test]
    fn no_re_review_for_first_time_request() {
        // Repro for #186104: you're in the pending review-request list but have
        // never reviewed the PR. That's a first-time request, not a re-review.
        let pr = pr_with_review_requests(&[Some("aorenste")]);
        assert!(!extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn re_review_requested_when_user_in_requests() {
        let pr = with_reviewers(
            pr_with_review_requests(&[Some("zou3519"), Some("aorenste")]),
            &["aorenste"],
        );
        assert!(extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn re_review_requested_is_case_insensitive() {
        // Case-insensitive on both the request match and the prior-review match.
        let pr = with_reviewers(pr_with_review_requests(&[Some("AOrenste")]), &["AORENSTE"]);
        assert!(extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn no_re_review_when_requested_but_only_others_reviewed() {
        // In the request list, but the only prior reviews are from other people
        // (the #186104 shape: kqfu/zou3519 reviewed, you didn't).
        let pr = with_reviewers(
            pr_with_review_requests(&[Some("aorenste")]),
            &["kqfu", "zou3519"],
        );
        assert!(!extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn no_re_review_when_user_absent() {
        let pr = pr_with_review_requests(&[Some("zou3519")]);
        assert!(!extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn no_re_review_when_no_requests() {
        let pr = pr_with_review_requests(&[]);
        assert!(!extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn team_request_without_login_ignored() {
        // A Team review request has no `login` field — must not panic or match.
        let pr = pr_with_review_requests(&[None]);
        assert!(!extract_re_review_requested(&pr, "aorenste"));
    }

    #[test]
    fn empty_user_never_re_review() {
        let pr = pr_with_review_requests(&[Some("aorenste")]);
        assert!(!extract_re_review_requested(&pr, ""));
    }

    #[test]
    fn re_review_propagates_through_convert_prs() {
        let pr = with_reviewers(pr_with_review_requests(&[Some("aorenste")]), &["aorenste"]);
        let inserts = convert_prs(std::slice::from_ref(&pr), "aorenste");
        assert!(inserts[0].re_review_requested);
    }

    #[test]
    fn manually_closed_pr_excluded_from_landed_display() {
        let nodes = vec![closed_pr(176170, false)];
        let landed = convert_landed_prs(&nodes);
        assert!(landed.is_empty(), "manually closed PRs should not appear in Recently Landed");
    }

    #[test]
    fn merged_pr_appears_in_landed_display() {
        let nodes = vec![closed_pr(100, true)];
        let landed = convert_landed_prs(&nodes);
        assert_eq!(landed.len(), 1);
    }

    #[test]
    fn manually_closed_pr_included_in_deletion_set() {
        let nodes = vec![closed_pr(176170, false), closed_pr(100, true)];
        let deletion_set = extract_all_closed(&nodes);
        assert_eq!(deletion_set.len(), 2, "all closed PRs should be in the deletion set");
        assert!(deletion_set.iter().any(|(_, n)| *n == 176170),
            "manually closed PR must be in the deletion set");
    }
}
