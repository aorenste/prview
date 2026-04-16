const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { prKey, ghstackParse, groupByStack } = require('./ghstack.js');

function makePr(number, repo, headRef, opts = {}) {
  return {
    number,
    repo,
    head_ref_name: headRef,
    base_ref_name: opts.base_ref_name || '',
    head_sha: opts.head_sha || '',
    base_sha: opts.base_sha || '',
    updated_at: opts.updated_at || '',
  };
}

describe('ghstackParse', () => {
  it('parses a valid ghstack head ref', () => {
    const r = ghstackParse('gh/user/42/head');
    assert.deepEqual(r, { prefix: 'gh/user/', num: 42, suffix: 'head' });
  });

  it('parses a valid ghstack base ref', () => {
    const r = ghstackParse('gh/user/42/base');
    assert.deepEqual(r, { prefix: 'gh/user/', num: 42, suffix: 'base' });
  });

  it('returns null for non-ghstack refs', () => {
    assert.equal(ghstackParse('main'), null);
    assert.equal(ghstackParse('feature/foo'), null);
    assert.equal(ghstackParse(''), null);
    assert.equal(ghstackParse(null), null);
  });
});

describe('groupByStack', () => {
  it('groups consecutive ghstack PRs into a stack', () => {
    const prs = [
      makePr(100, 'foo/bar', 'gh/user/1/head', { head_sha: 'aaa' }),
      makePr(101, 'foo/bar', 'gh/user/2/head', { head_sha: 'bbb', base_sha: 'aaa' }),
    ];
    const items = groupByStack(prs);
    assert.equal(items.length, 1);
    assert.equal(items[0].type, 'stack');
    assert.deepEqual(items[0].prs.map(p => p.number), [100, 101]);
  });

  it('does NOT group consecutive numbers from different stacks (SHA mismatch)', () => {
    // PR 180140 has head gh/user/5/head, sha "aaa"
    // PR 180141 has head gh/user/6/head, sha "ccc", base_sha "bbb"
    // base_sha "bbb" != parent head_sha "aaa" -> different stacks
    const prs = [
      makePr(180140, 'foo/bar', 'gh/user/5/head', { head_sha: 'aaa' }),
      makePr(180141, 'foo/bar', 'gh/user/6/head', { head_sha: 'ccc', base_sha: 'bbb' }),
    ];
    const items = groupByStack(prs);
    assert.equal(items.length, 2, 'should be 2 singles, not 1 stack');
    assert.equal(items[0].type, 'single');
    assert.equal(items[1].type, 'single');
  });

  it('does NOT group when parent has no head_sha (My PRs tab bug)', () => {
    // This is the actual bug: PrRow doesn't include head_sha,
    // so parent.head_sha is undefined/"", and the SHA check is skipped.
    // Two unrelated consecutive ghstack PRs get falsely grouped.
    const prs = [
      makePr(180140, 'foo/bar', 'gh/user/5/head', { head_sha: '' }),
      makePr(180141, 'foo/bar', 'gh/user/6/head', { head_sha: '', base_sha: 'bbb' }),
    ];
    const items = groupByStack(prs);
    assert.equal(items.length, 2, 'should be 2 singles when head_sha is missing');
    assert.equal(items[0].type, 'single');
    assert.equal(items[1].type, 'single');
  });

  it('keeps non-ghstack PRs as singles', () => {
    const prs = [
      makePr(1, 'foo/bar', 'main'),
      makePr(2, 'foo/bar', 'feature/x'),
    ];
    const items = groupByStack(prs);
    assert.equal(items.length, 2);
    assert.equal(items[0].type, 'single');
    assert.equal(items[1].type, 'single');
  });

  it('does not group ghstack PRs from different repos', () => {
    const prs = [
      makePr(100, 'foo/bar', 'gh/user/1/head', { head_sha: 'aaa' }),
      makePr(101, 'other/repo', 'gh/user/2/head', { head_sha: 'bbb', base_sha: 'aaa' }),
    ];
    const items = groupByStack(prs);
    assert.equal(items.length, 2);
    assert.equal(items[0].type, 'single');
    assert.equal(items[1].type, 'single');
  });
});
