// Pure-logic helpers for ghstack grouping. Shared by app.js and tests.
// In the browser these are loaded via <script> before app.js.
// In Node they are require()'d.

function prKey(pr) { return pr.repo + '#' + pr.number; }

// --- ghstack grouping ---
// ghstack uses branches: gh/<user>/<N>/head (PR branch) and gh/<user>/<N>/base (target branch).
// Each PR targets its own <N>/base. The ghstack tool sets <N>/base == <N-1>/head, forming a chain.
// So if PR A has head=gh/user/X/head and PR B has base=gh/user/X/base, then B is stacked on A
// (same X means B targets A's content as its base).
function ghstackParse(refName) {
  const m = (refName || '').match(/^(gh\/[^/]+\/)(\d+)\/(head|base)$/);
  return m ? { prefix: m[1], num: parseInt(m[2], 10), suffix: m[3] } : null;
}

function ghstackPosition(pr) {
  const p = ghstackParse(pr.head_ref_name);
  return p ? p.num : 0;
}

// Group PRs into ghstack chains.
// Link: PR B (base=gh/user/X/base) stacks on PR A (head=gh/user/X/head) when X matches.
// Returns [{type:'stack', id, prs, maxUpdated} | {type:'single', pr, maxUpdated}]
function groupByStack(prs) {
  // Index ghstack PRs by (repo, prefix, num) from their head branch
  const byHeadNum = new Map();
  for (const pr of prs) {
    const p = ghstackParse(pr.head_ref_name);
    if (p && p.suffix === 'head') {
      byHeadNum.set(pr.repo + ':' + p.prefix + ':' + p.num, pr);
    }
  }

  // Build adjacency: PR with head num N is stacked on PR with head num (N-1),
  // same prefix and same repo. When SHA data is available, verify the link:
  // child's base_sha must match parent's head_sha (proves same ghstack chain).
  const parentOf = new Map(); // prKey -> parent PR
  const childOf = new Map();  // prKey -> child PR
  for (const pr of prs) {
    const p = ghstackParse(pr.head_ref_name);
    if (!p || p.suffix !== 'head') continue;
    const parentKey = pr.repo + ':' + p.prefix + ':' + (p.num - 1);
    const parent = byHeadNum.get(parentKey);
    if (parent) {
      // Verify via commit SHAs (prevents cross-stack false links).
      // If either SHA is missing we can't verify, so don't link.
      if (!pr.base_sha || !parent.head_sha || pr.base_sha !== parent.head_sha) continue;
      parentOf.set(prKey(pr), parent);
      childOf.set(prKey(parent), pr);
    }
  }

  // Walk chains from roots (PRs with no parent)
  const inChain = new Set();
  const chains = [];
  for (const pr of prs) {
    const p = ghstackParse(pr.head_ref_name);
    if (!p || p.suffix !== 'head') continue;
    if (parentOf.has(prKey(pr))) continue; // not a root
    if (!childOf.has(prKey(pr))) continue; // isolated, no children

    const chain = [pr];
    let current = pr;
    while (childOf.has(prKey(current))) {
      current = childOf.get(prKey(current));
      chain.push(current);
    }
    if (chain.length >= 2) {
      const maxUpdated = chain.reduce((max, p) =>
        (p.updated_at || '') > max ? p.updated_at : max, '');
      const id = chain.map(p => p.number).join('-');
      chains.push({type: 'stack', id, prs: chain, maxUpdated});
      for (const p of chain) inChain.add(prKey(p));
    }
  }

  // Build items preserving the input sort order.
  // Each stack group appears at the position of its first member in `prs`.
  const chainByPr = new Map(); // prKey -> chain item
  for (const chain of chains) {
    for (const p of chain.prs) chainByPr.set(prKey(p), chain);
  }
  const items = [];
  const emitted = new Set();
  for (const pr of prs) {
    const k = prKey(pr);
    if (inChain.has(k)) {
      const chain = chainByPr.get(k);
      if (!emitted.has(chain.id)) {
        emitted.add(chain.id);
        items.push(chain);
      }
    } else {
      items.push({type: 'single', pr, maxUpdated: pr.updated_at || ''});
    }
  }
  return items;
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { prKey, ghstackParse, ghstackPosition, groupByStack };
}
