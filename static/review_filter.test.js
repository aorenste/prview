// Filter tests for the "Needs Attention" (reviews) tab.
// Drives data in through applyUpdate() — the same path the SSE update event
// uses — then asserts which rows survive the default filters.

const { test } = require('node:test');
const assert = require('node:assert');
const { loadApp } = require('./test_helpers');

function reviewPr(overrides) {
  return Object.assign({
    repo: 'pytorch/pytorch',
    number: 1,
    title: 'A PR',
    url: 'https://github.com/pytorch/pytorch/pull/1',
    author: 'someone',
    state: 'OPEN',
    updated_at: '2026-05-01T00:00:00Z',
    review_status: '',
    reviewers: '[]',
    is_draft: false,
    is_read: false,
    is_mentioned: false,
    re_review_requested: false,
    ci_approval_needed: false,
    drci_emoji: '',
    drci_status: '',
    drci_updated_at: '',
    checks_overall: '',
    checks_running: false,
    comment_count: 0,
  }, overrides);
}

function minutesAgo(n) {
  return new Date(Date.now() - n * 60 * 1000).toISOString();
}

function feedReviews(win, prs) {
  win.applyUpdate({
    target_user: 'aorenste',
    pr_updates: [],
    review_updates: prs.map(p => Object.assign({ type: 'changed' }, p)),
    issue_updates: [],
    hidden_count: 0,
  });
}

function rowKeys(win) {
  return Array.from(win.document.querySelectorAll('#reviews-body tr[data-key]'))
    .map(tr => tr.getAttribute('data-key'));
}

test('approved PR with a pending review request stays hidden (not mentioned)', () => {
  // Real-world repro: PR #175453 — approved by someone else, you have a
  // pending review request but never reviewed it, and you are not mentioned.
  // With "Show approved" off (the default) it should NOT appear.
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 175453,
    review_status: 'APPROVED',
    re_review_requested: true,
  })]);

  assert.ok(
    !rowKeys(win).includes('pytorch/pytorch#175453'),
    'an approved PR should be hidden even when a review is pending, unless mentioned'
  );
});

test('changes-requested PR with a re-review request stays visible', () => {
  // The case the re-review flag was built for: a stale CHANGES_REQUESTED still
  // drives reviewDecision, but the ball is back in your court. With "Show
  // changes-requested" off (the default) it should still appear.
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 200,
    review_status: 'CHANGES_REQUESTED',
    re_review_requested: true,
  })]);

  assert.ok(
    rowKeys(win).includes('pytorch/pytorch#200'),
    'a re-requested changes-requested PR should remain visible'
  );
});

test('approved PR you were mentioned in stays visible', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 300,
    review_status: 'APPROVED',
    is_mentioned: true,
  })]);

  assert.ok(
    rowKeys(win).includes('pytorch/pytorch#300'),
    'a mention should force an approved PR visible'
  );
});

// --- "Only show passing" filter ---
//
// Spec (in priority order):
//  1. CI still building            -> not passing (hidden)
//  2. CI fully green               -> passing (shown)
//  3+ CI has red -> defer to DrCI:
//  3. DrCI says passing            -> shown
//  4. DrCI says failing            -> hidden
//  5. DrCI no verdict, >20m stale  -> indeterminate, shown (so it isn't lost)
//  6. DrCI no verdict, recent      -> still catching up = building, hidden

function passingOnly() {
  return loadApp({ prefs: { showPassing: true } });
}

test('Only show passing: still-building PR is hidden (rule 1)', () => {
  // Repro for #184133: rollup PENDING, DrCI has not posted a verdict yet.
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 184133,
    checks_overall: 'PENDING',
    checks_running: true,
  })]);

  assert.ok(
    !rowKeys(win).includes('pytorch/pytorch#184133'),
    'a building PR should not appear under "Only show passing"'
  );
});

test('Only show passing: fully green PR is shown (rule 2)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({ number: 2, checks_overall: 'SUCCESS' })]);

  assert.ok(rowKeys(win).includes('pytorch/pytorch#2'),
    'a fully green PR should appear under "Only show passing"');
});

test('Only show passing: green wins even if a check is still running (rule 1 before 2)', () => {
  // A PR that is still running is building regardless of how many checks passed.
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 3,
    checks_overall: 'PENDING',
    checks_running: true,
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#3'),
    'a running PR is building, not passing');
});

test('Only show passing: red CI + DrCI says pass is shown (rule 3)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 4,
    checks_overall: 'FAILURE',
    drci_emoji: 'white_check_mark',
    drci_updated_at: minutesAgo(2),
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#4'),
    'red CI that DrCI clears as passing should be shown');
});

test('Only show passing: red CI + DrCI says fail is hidden (rule 4)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 5,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_updated_at: minutesAgo(2),
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#5'),
    'red CI that DrCI confirms as failing should be hidden');
});

test('Only show passing: red CI + no DrCI verdict, stale DrCI is shown (rule 5)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 6,
    checks_overall: 'FAILURE',
    drci_emoji: '',
    drci_updated_at: minutesAgo(30),
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#6'),
    'when DrCI is stale (>20m) and has no verdict, surface the PR');
});

test('Only show passing: red CI + no DrCI verdict, recent DrCI is hidden (rule 6)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 7,
    checks_overall: 'FAILURE',
    drci_emoji: '',
    drci_updated_at: minutesAgo(5),
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#7'),
    'a recently-updated DrCI with no verdict means we are still waiting (building)');
});

test('Only show passing: a mention is always shown', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 8,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    is_mentioned: true,
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#8'),
    'a mention bypasses the passing filter');
});

// --- CI approval needed is a "ball in your court" signal ---
// When CI requires your approval to run, the PR needs your action, so it must
// stay visible past the state hides (changes-requested / approved) and the
// passing filter.

test('CI-approval-needed PR stays visible past the changes-requested hide', () => {
  // Repro for #167224: CHANGES_REQUESTED (so the default hide applies) but it
  // needs your CI approval. showRejected is off by default.
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 167224,
    review_status: 'CHANGES_REQUESTED',
    ci_approval_needed: true,
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#167224'),
    'a PR needing CI approval should override the changes-requested hide');
});

test('changes-requested PR without CI approval is still hidden', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 9,
    review_status: 'CHANGES_REQUESTED',
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#9'),
    'a plain changes-requested PR should still be hidden by default');
});

test('CI-approval-needed PR stays visible under Only show passing', () => {
  // Even if its rollup is green, a PR awaiting your CI approval needs action.
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 10,
    checks_overall: 'PENDING',
    ci_approval_needed: true,
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#10'),
    'a PR needing CI approval should override the passing filter');
});

test('CI-approval-needed PR stays visible past the approved hide', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 11,
    review_status: 'APPROVED',
    ci_approval_needed: true,
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#11'),
    'a PR needing CI approval should override the approved hide');
});
