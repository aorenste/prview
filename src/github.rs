use serde::Deserialize;

use crate::db::PrInsert;

const MY_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state } }
  commits(last: 1) {
    nodes { commit { statusCheckRollup {
      contexts(first: 100) {
        totalCount
        nodes {
          ... on CheckRun { status conclusion }
          ... on StatusContext { state }
        }
      }
    } } }
  }
  comments(first: 100) { nodes { author { login } body } }
";

const REVIEW_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state } }
  commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }
  comments { totalCount }
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
    nodes: Vec<GqlPr>,
}

#[derive(Deserialize)]
struct GqlPr {
    number: i64,
    title: String,
    url: String,
    author: Option<GqlAuthor>,
    #[serde(rename = "isDraft")]
    is_draft: bool,
    repository: GqlRepo,
    state: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    #[serde(rename = "reviewDecision")]
    review_decision: Option<String>,
    reviews: GqlNodes<GqlReview>,
    commits: Option<GqlNodes<GqlCommitNode>>,
    comments: Option<GqlNodes<GqlComment>>,
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct GqlNodes<T> {
    #[serde(default)]
    nodes: Vec<T>,
    #[serde(default, rename = "totalCount")]
    total_count: Option<i64>,
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
}

#[derive(Deserialize)]
struct GqlStatusCheckRollup {
    state: Option<String>,
    contexts: Option<GqlCheckContexts>,
}

#[derive(Deserialize)]
struct GqlCheckContexts {
    #[serde(rename = "totalCount")]
    total_count: i64,
    nodes: Vec<GqlCheckNode>,
}

#[derive(Deserialize)]
struct GqlCheckNode {
    // CheckRun fields
    status: Option<String>,
    conclusion: Option<String>,
    // StatusContext field
    state: Option<String>,
}

#[derive(Deserialize)]
struct GqlComment {
    author: GqlAuthor,
    body: String,
}

pub struct FetchResult {
    pub my_prs: Vec<PrInsert>,
    pub review_prs: Vec<PrInsert>,
}

fn make_query(search_filter: &str, fields: &str) -> String {
    format!(
        r#"query {{
  search(query: "{}", type: ISSUE, first: 50) {{
    nodes {{
      ... on PullRequest {{
        {}
      }}
    }}
  }}
}}"#,
        search_filter, fields
    )
}

async fn run_query(search_filter: &str, fields: &str) -> Result<Vec<GqlPr>, Box<dyn std::error::Error + Send + Sync>> {
    let query = make_query(search_filter, fields);
    let output = tokio::process::Command::new("gh")
        .args(["api", "graphql", "-f", &format!("query={}", query)])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("GraphQL query failed: {}", stderr).into());
    }

    let resp: GqlResponse = serde_json::from_slice(&output.stdout)?;
    Ok(resp.data.search.nodes)
}

fn convert_prs(nodes: &[GqlPr]) -> Vec<PrInsert> {
    nodes.iter().map(|pr| {
        let (review_status, reviewers) = extract_reviews(pr);
        let (checks_success, checks_fail, checks_pending) = extract_checks(pr);
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
            review_status,
            reviewers,
            checks_overall: extract_checks_overall(pr),
            checks_success,
            checks_fail,
            checks_pending,
            drci_status,
            drci_emoji,
            comment_count,
            head_sha: extract_head_sha(pr),
        }
    }).collect()
}

pub async fn fetch_all_prs() -> Result<FetchResult, Box<dyn std::error::Error + Send + Sync>> {
    let (my_nodes, review_nodes) = tokio::try_join!(
        run_query("is:pr is:open author:@me", MY_PR_FIELDS),
        run_query("is:pr is:open review-requested:@me", REVIEW_PR_FIELDS),
    )?;

    Ok(FetchResult {
        my_prs: convert_prs(&my_nodes),
        review_prs: convert_prs(&review_nodes),
    })
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

fn extract_checks(pr: &GqlPr) -> (i64, i64, i64) {
    let commits = match &pr.commits {
        Some(c) => c,
        None => return (0, 0, 0),
    };
    let commit = match commits.nodes.first() {
        Some(c) => c,
        None => return (0, 0, 0),
    };
    let rollup = match &commit.commit.status_check_rollup {
        Some(r) => r,
        None => return (0, 0, 0),
    };

    let contexts = match &rollup.contexts {
        Some(c) => c,
        None => return (0, 0, 0),
    };

    let total_count = contexts.total_count;
    let mut success: i64 = 0;
    let mut fail: i64 = 0;
    let mut pending: i64 = 0;

    for check in &contexts.nodes {
        // CheckRun: has status + conclusion
        if let Some(conclusion) = &check.conclusion {
            match conclusion.as_str() {
                "SUCCESS" | "NEUTRAL" | "SKIPPED" => success += 1,
                "FAILURE" | "CANCELLED" | "TIMED_OUT" | "ACTION_REQUIRED" | "STARTUP_FAILURE" | "ERROR" => fail += 1,
                _ => pending += 1,
            }
        } else if let Some(status) = &check.status {
            // CheckRun with no conclusion yet
            match status.as_str() {
                "COMPLETED" => success += 1,
                _ => pending += 1,
            }
        } else if let Some(state) = &check.state {
            // StatusContext
            match state.as_str() {
                "SUCCESS" | "NEUTRAL" => success += 1,
                "FAILURE" | "ERROR" => fail += 1,
                _ => pending += 1,
            }
        }
    }

    // Account for checks beyond the first 100 we fetched
    let fetched = contexts.nodes.len() as i64;
    let unfetched = total_count - fetched;
    if unfetched > 0 {
        // We don't know their state, count as pending
        pending += unfetched;
    }

    (success, fail, pending)
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

fn extract_comment_count(pr: &GqlPr) -> i64 {
    match &pr.comments {
        Some(c) if !c.nodes.is_empty() => c.nodes.iter()
            .filter(|c| !matches!(c.author.login.as_str(), "pytorch-bot" | "facebook-github-bot"))
            .count() as i64,
        Some(c) => c.total_count.unwrap_or(0),
        None => 0,
    }
}
