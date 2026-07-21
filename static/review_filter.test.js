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
    drci_ai_verdict: '',
    checks_overall: '',
    checks_running: false,
    comment_count: 0,
    additions: 0,
    deletions: 0,
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

function dimmedKeys(win) {
  return Array.from(win.document.querySelectorAll('#reviews-body tr.debug-hidden'))
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

test('draft + re-review request stays hidden when Show drafts is off', () => {
  // Repro for #181125: you reviewed it before and are re-requested, but it's a
  // draft — a draft isn't reviewable, so the draft hide should win.
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 181125,
    is_draft: true,
    re_review_requested: true,
  })]);

  assert.ok(
    !rowKeys(win).includes('pytorch/pytorch#181125'),
    'a draft should stay hidden even when a re-review was requested'
  );
});

test('draft + mention still shows when Show drafts is off', () => {
  // A direct @-mention is a strong enough signal to surface even a draft.
  const win = loadApp();
  feedReviews(win, [reviewPr({ number: 1, is_draft: true, is_mentioned: true })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#1'),
    'a mention should still surface a draft');
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
//  4. DrCI says failing, but AI advisor says every failure is "not related" -> shown
//  5. DrCI says failing            -> hidden
//  6. DrCI no verdict, >20m stale  -> indeterminate, shown (so it isn't lost)
//  7. DrCI no verdict, recent      -> still catching up = building, hidden

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

function aiBlob({ related = 0, not_related = 0, uncertain = 0, pending = 0 } = {}) {
  return JSON.stringify({ related, not_related, uncertain, pending });
}

test('Only show passing: red DrCI whose failures the AI calls "not related" is shown (rule 4)', () => {
  // Repro for pytorch/pytorch#189486: DrCI shows :x: for an unclassified failure
  // but the AI advisor judged it "not related", so it's effectively mergeable.
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 189486,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_ai_verdict: aiBlob({ not_related: 2 }),
    drci_updated_at: minutesAgo(2),
  })]);
  assert.ok(rowKeys(win).includes('pytorch/pytorch#189486'),
    'a red DrCI whose failures are all AI "not related" should be shown as passing');
});

test('Only show passing: AI "related" verdict keeps a red DrCI hidden (rule 5)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 51,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_ai_verdict: aiBlob({ related: 1, not_related: 1 }),
    drci_updated_at: minutesAgo(2),
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#51'),
    'any AI "related" verdict means the failure could be real -> hidden');
});

test('Only show passing: an uncertain/analyzing AI verdict keeps a red DrCI hidden (rule 5)', () => {
  const win = passingOnly();
  feedReviews(win, [
    reviewPr({ number: 52, checks_overall: 'FAILURE', drci_emoji: 'x',
      drci_ai_verdict: aiBlob({ not_related: 1, uncertain: 1 }), drci_updated_at: minutesAgo(2) }),
    reviewPr({ number: 53, checks_overall: 'FAILURE', drci_emoji: 'x',
      drci_ai_verdict: aiBlob({ not_related: 1, pending: 1 }), drci_updated_at: minutesAgo(2) }),
  ]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#52'),
    'an uncertain verdict is not confident enough to call the PR passing');
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#53'),
    'a still-analyzing verdict is not a conclusion yet');
});

test('Only show passing: red CI + DrCI says fail with no AI verdict is hidden (rule 5)', () => {
  const win = passingOnly();
  feedReviews(win, [reviewPr({
    number: 5,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_updated_at: minutesAgo(2),
  })]);
  assert.ok(!rowKeys(win).includes('pytorch/pytorch#5'),
    'red CI that DrCI confirms as failing (no AI verdict) should be hidden');
});

test('DrCI cell shows a "Not related" pill and AI tooltip when the AI clears a red DrCI', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 60,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_status: '1 Unclassified Failure',
    drci_ai_verdict: aiBlob({ not_related: 1 }),
  })]);
  const row = win.document.querySelector('#reviews-body tr[data-key="pytorch/pytorch#60"]');
  assert.ok(row, 'the PR row should be visible');
  assert.match(row.innerHTML, /Not related/, 'renders the softer AI pill label');
  assert.match(row.innerHTML, /AI: 1 not related/, 'the tooltip includes the AI summary');
});

test('Needs Attention: CI column is folded into the DrCI pill hover, no separate CI cell', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({
    number: 61,
    checks_overall: 'FAILURE',
    drci_emoji: 'x',
    drci_status: '1 New Failure',
    checks_success: 10, checks_fail: 2, checks_pending: 3,
  })]);
  // Header renamed DrCI -> CI, and the standalone CI column is gone.
  const headers = Array.from(win.document.querySelectorAll('#reviews-thead th'))
    .map(th => th.textContent.replace(/[▲▴▼]/g, '').trim());
  assert.ok(headers.includes('CI'), 'the DrCI column is relabeled "CI"');
  assert.ok(headers.includes('Effort'), 'the Effort column is present');
  assert.strictEqual(headers.filter(h => h === 'CI').length, 1, 'only one CI column');
  // The raw CI check counts now live in the CI pill tooltip.
  const row = win.document.querySelector('#reviews-body tr[data-key="pytorch/pytorch#61"]');
  assert.match(row.innerHTML, /CI: 10 passed, 2 failed, 3 pending/,
    'the CI check counts are folded into the pill tooltip');
});

test('Needs Attention: Effort cell shows +add -del colored by review difficulty', () => {
  const win = loadApp();
  feedReviews(win, [
    reviewPr({ number: 185648, additions: 31, deletions: 26 }),   // 57 -> easy
    reviewPr({ number: 71, additions: 400, deletions: 50 }),      // 450 -> medium
    reviewPr({ number: 72, additions: 900, deletions: 100 }),     // 1000 -> hard
    reviewPr({ number: 73, additions: 0, deletions: 0 }),         // none
  ]);
  const cell = (n) => win.document
    .querySelector(`#reviews-body tr[data-key="pytorch/pytorch#${n}"] .effort`);
  assert.strictEqual(cell(185648).textContent, '+31 -26', 'renders +added -deleted');
  assert.match(cell(185648).getAttribute('style'), /var\(--green\)/, 'small diff is green (easy)');
  assert.match(cell(71).getAttribute('style'), /var\(--yellow\)/, 'medium diff is yellow');
  assert.match(cell(72).getAttribute('style'), /var\(--red\)/, 'large diff is red (hard)');
  assert.strictEqual(cell(73).textContent, '—', 'no changes renders an em dash');
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

// --- Debug mode ---
// When on, every review PR renders and the ones the filters would hide are
// dimmed (class debug-hidden) instead of removed.

test('debug off: filtered-out PRs are removed (default)', () => {
  const win = loadApp();
  feedReviews(win, [
    reviewPr({ number: 1, review_status: '' }),            // visible
    reviewPr({ number: 2, review_status: 'APPROVED' }),    // hidden by default
  ]);
  assert.deepStrictEqual(rowKeys(win), ['pytorch/pytorch#1']);
  assert.deepStrictEqual(dimmedKeys(win), []);
});

test('debug on: all PRs render; filtered-out ones are dimmed', () => {
  const win = loadApp({ prefs: { debugMode: true } });
  feedReviews(win, [
    reviewPr({ number: 1, review_status: '' }),            // would stay visible
    reviewPr({ number: 2, review_status: 'APPROVED' }),    // would be hidden
  ]);
  // Both rows present...
  assert.deepStrictEqual(rowKeys(win).sort(),
    ['pytorch/pytorch#1', 'pytorch/pytorch#2']);
  // ...but only the filtered-out one is dimmed.
  assert.deepStrictEqual(dimmedKeys(win), ['pytorch/pytorch#2']);
});

test('debug on: a PR an override keeps visible is not dimmed', () => {
  // CI-approval overrides the approved hide, so it stays a "real" visible row.
  const win = loadApp({ prefs: { debugMode: true } });
  feedReviews(win, [
    reviewPr({ number: 11, review_status: 'APPROVED', ci_approval_needed: true }),
  ]);
  assert.deepStrictEqual(rowKeys(win), ['pytorch/pytorch#11']);
  assert.deepStrictEqual(dimmedKeys(win), []);
});

test('debug toggle button is present when there are review PRs', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({ number: 1 })]);
  assert.ok(win.document.getElementById('debug-toggle'),
    'the bug-icon debug toggle should render in the filter bar');
});

// --- Hide-by-author (jansel) ---
// A "Hide N from jansel" toggle, default off. When on, mutes that author.

test('hide-author off (default): jansel PRs show', () => {
  const win = loadApp();
  feedReviews(win, [
    reviewPr({ number: 1, author: 'jansel' }),
    reviewPr({ number: 2, author: 'someone' }),
  ]);
  assert.deepStrictEqual(rowKeys(win).sort(),
    ['pytorch/pytorch#1', 'pytorch/pytorch#2']);
});

test('hide-author on: jansel PRs are hidden, others stay', () => {
  const win = loadApp({ prefs: { hideAuthor: true } });
  feedReviews(win, [
    reviewPr({ number: 1, author: 'jansel' }),
    reviewPr({ number: 2, author: 'someone' }),
  ]);
  assert.deepStrictEqual(rowKeys(win), ['pytorch/pytorch#2']);
});

test('hide-author on: a mention does NOT override the author mute', () => {
  const win = loadApp({ prefs: { hideAuthor: true } });
  feedReviews(win, [reviewPr({ number: 1, author: 'jansel', is_mentioned: true })]);
  assert.deepStrictEqual(rowKeys(win), []);
});

test('debug on + hide-author on: jansel PR renders but dimmed', () => {
  const win = loadApp({ prefs: { hideAuthor: true, debugMode: true } });
  feedReviews(win, [
    reviewPr({ number: 1, author: 'jansel' }),
    reviewPr({ number: 2, author: 'someone' }),
  ]);
  assert.deepStrictEqual(rowKeys(win).sort(),
    ['pytorch/pytorch#1', 'pytorch/pytorch#2']);
  assert.deepStrictEqual(dimmedKeys(win), ['pytorch/pytorch#1']);
});

test('hide-author chip is present when that author has a PR', () => {
  const win = loadApp();
  feedReviews(win, [reviewPr({ number: 1, author: 'jansel' })]);
  assert.ok(win.document.getElementById('hide-author-toggle'),
    'the hide-author toggle should render when the author has a review PR');
});
