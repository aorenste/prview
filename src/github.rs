use serde::Deserialize;

use crate::db::PrInsert;

const MY_PR_FIELDS: &str = "
  number title url
  author { login }
  isDraft
  repository { nameWithOwner }
  state createdAt updatedAt reviewDecision
  reviews(first: 20) { nodes { author { login } state } }
  commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }
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
                .await?;

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
            review_status,
            reviewers,
            checks_overall: extract_checks_overall(pr),
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
    let (my_nodes, review_nodes) = tokio::try_join!(
        run_query(&my_query, MY_PR_FIELDS),
        run_query(&review_query, REVIEW_PR_FIELDS),
    )?;

    let mut review_prs = convert_prs(&review_nodes);

    // Check CI approval status for review PRs in parallel
    let futures: Vec<_> = review_prs.iter()
        .map(|pr| check_ci_approval_needed(&pr.repo, &pr.head_sha))
        .collect();
    let results = futures::future::join_all(futures).await;
    for (pr, needed) in review_prs.iter_mut().zip(results) {
        pr.ci_approval_needed = needed;
    }

    Ok(FetchResult {
        my_prs: convert_prs(&my_nodes),
        review_prs,
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
