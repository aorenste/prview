use serde::Deserialize;

use crate::db::PrInsert;

const MY_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  headRefName baseRefName
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
  headRefName baseRefName
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state } }
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
    #[serde(default, rename = "headRefName")]
    head_ref_name: String,
    #[serde(default, rename = "baseRefName")]
    base_ref_name: String,
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
struct GqlReview {
    author: GqlAuthor,
    state: String,
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
                eprintln!("Retrying issue GraphQL query (attempt {})", attempt + 1);
            }
            let output = tokio::process::Command::new("gh")
                .args(["api", "graphql", "-f", &format!("query={}", query)])
                .output()
                .await
                .map_err(|e| format!("Failed to run 'gh': {}", e))?;

            if !output.status.success() {
                last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
                continue;
            }

            match serde_json::from_slice::<GqlIssueResponse>(&output.stdout) {
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
                eprintln!("Retrying GraphQL query (attempt {})", attempt + 1);
            }
            let output = tokio::process::Command::new("gh")
                .args(["api", "graphql", "-f", &format!("query={}", query)])
                .output()
                .await
                .map_err(|e| format!("Failed to run 'gh': {}", e))?;

            if !output.status.success() {
                last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
                continue;
            }

            match serde_json::from_slice::<GqlResponse>(&output.stdout) {
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

fn convert_prs(nodes: &[GqlPr]) -> Vec<PrInsert> {
    nodes.iter().map(|pr| {
        let (review_status, reviewers) = extract_reviews(pr);
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
            ci_approval_needed: false,
        }
    }).collect()
}

async fn check_ci_approval_needed(repo: &str, head_sha: &str) -> bool {
    if head_sha.is_empty() {
        return false;
    }
    let endpoint = format!("repos/{}/actions/runs?head_sha={}&status=action_required&per_page=1", repo, head_sha);
    let output = tokio::process::Command::new("gh")
        .args(["api", &endpoint, "--jq", ".total_count"])
        .output()
        .await;
    match output {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout);
            s.trim().parse::<i64>().unwrap_or(0) > 0
        }
        _ => false,
    }
}

pub async fn fetch_all_prs(user: &str) -> Result<FetchResult, Box<dyn std::error::Error + Send + Sync>> {
    let user_filter = if user.is_empty() { "@me".to_string() } else { user.to_string() };
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

    let mut review_prs = convert_prs(&all_review_nodes);

    // Check CI approval status for review PRs in parallel
    let futures: Vec<_> = review_prs.iter()
        .map(|pr| check_ci_approval_needed(&pr.repo, &pr.head_sha))
        .collect();
    let results = futures::future::join_all(futures).await;
    for (pr, needed) in review_prs.iter_mut().zip(results) {
        pr.ci_approval_needed = needed;
    }

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
    let (merged_prs, issues) = tokio::join!(
        async {
            match run_query(&landed_query, LANDED_PR_FIELDS).await {
                Ok(nodes) => convert_landed_prs(&nodes),
                Err(e) => {
                    eprintln!("Warning: failed to fetch landed PRs: {}", e);
                    vec![]
                }
            }
        },
        async {
            match run_issue_query(&issue_query, ISSUE_FIELDS).await {
                Ok(nodes) => convert_issues(&nodes),
                Err(e) => {
                    eprintln!("Warning: failed to fetch issues: {}", e);
                    vec![]
                }
            }
        }
    );

    Ok(FetchResult {
        my_prs: convert_prs(&my_nodes),
        review_prs,
        merged_prs,
        issues,
    })
}

fn days_to_ymd(days_since_epoch: i64) -> (i64, i64, i64) {
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
                if line.starts_with("## :") && !line.contains("Helpful Links") {
                    if let Some(rest) = line.strip_prefix("## :") {
                        if let Some(colon_pos) = rest.find(':') {
                            let emoji = rest[..colon_pos].to_string();
                            let status = rest[colon_pos + 1..].trim().to_string();
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

pub async fn fetch_pr_details(repo: &str, number: i64, include_landing: bool) -> Result<PrDetailUpdate, Box<dyn std::error::Error + Send + Sync>> {
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
      comments(last:100) {{ nodes {{ author {{ login }} body createdAt }} }}
    }}
  }}
}}"#,
        owner, name, number
    );

    let output = tokio::process::Command::new("gh")
        .args(["api", "graphql", "-f", &format!("query={}", query)])
        .output()
        .await
        .map_err(|e| format!("Failed to run 'gh': {}", e))?;

    if !output.status.success() {
        return Err(format!("GraphQL error: {}", String::from_utf8_lossy(&output.stderr)).into());
    }

    let resp: DetailResponse = serde_json::from_slice(&output.stdout)?;
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

                let page_output = tokio::process::Command::new("gh")
                    .args(["api", "graphql", "-f", &format!("query={}", page_query)])
                    .output()
                    .await
                    .map_err(|e| format!("Failed to run 'gh': {}", e))?;

                if !page_output.status.success() {
                    break;
                }

                let page_resp: ContextPageResponse = serde_json::from_slice(&page_output.stdout)?;
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

    // Count CI results
    let (mut success, mut fail, mut pending) = (0i64, 0i64, 0i64);
    for node in &all_context_nodes {
        if let Some(conclusion) = node.get("conclusion") {
            // CheckRun
            if conclusion.is_null() {
                pending += 1; // still running
            } else {
                match conclusion.as_str().unwrap_or("") {
                    "SUCCESS" | "NEUTRAL" | "SKIPPED" => success += 1,
                    "FAILURE" | "TIMED_OUT" | "CANCELLED" | "ACTION_REQUIRED" => fail += 1,
                    _ => pending += 1,
                }
            }
        } else if let Some(state) = node.get("state").and_then(|s| s.as_str()) {
            // StatusContext
            match state {
                "SUCCESS" => success += 1,
                "FAILURE" | "ERROR" => fail += 1,
                "PENDING" => pending += 1,
                _ => pending += 1,
            }
        }
    }

    // Extract DrCI from comments
    let (drci_emoji, drci_status) = extract_drci_from_detail_comments(&pr.comments.nodes);

    // Extract landing status from pytorchmergebot comments
    let landing_status = if include_landing {
        extract_landing_status(&pr.comments.nodes, committed_date.as_deref())
    } else {
        String::new()
    };

    Ok(PrDetailUpdate {
        checks_success: success,
        checks_fail: fail,
        checks_pending: pending,
        checks_running: pending > 0,
        drci_emoji,
        drci_status,
        landing_status,
    })
}

fn extract_drci_from_detail_comments(comments: &[DetailComment]) -> (String, String) {
    for comment in comments {
        let login = comment.author.as_ref().map(|a| a.login.as_str()).unwrap_or("");
        if login != "pytorch-bot" && login != "pytorch-bot[bot]" {
            continue;
        }
        if !comment.body.contains("drci-comment-start") {
            continue;
        }
        for line in comment.body.lines() {
            if line.starts_with("## :") && !line.contains("Helpful Links") {
                if let Some(rest) = line.strip_prefix("## :") {
                    if let Some(colon_pos) = rest.find(':') {
                        let emoji = rest[..colon_pos].to_string();
                        let status = rest[colon_pos + 1..].trim().to_string();
                        return (emoji, status);
                    }
                }
            }
        }
        break;
    }
    (String::new(), String::new())
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
