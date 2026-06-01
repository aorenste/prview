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
    drci_emoji: '',
    drci_status: '',
    comment_count: 0,
  }, overrides);
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
