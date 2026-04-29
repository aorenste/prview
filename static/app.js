// --- Responsive ---
const narrowMq = window.matchMedia('(max-width: 750px)');
const mediumMq = window.matchMedia('(max-width: 1000px)');
let isNarrow = narrowMq.matches;
let isMedium = mediumMq.matches;
narrowMq.addEventListener('change', e => { isNarrow = e.matches; renderAll(); });
mediumMq.addEventListener('change', e => { isMedium = e.matches; renderAll(); });

// --- State ---
let buildHash = null;
let allPrs = [];
let allReviewPrs = [];
let allMergedPrs = [];
let allIssues = [];
let hiddenCount = 0;
let hasFetched = false;


// Persist toggle state in localStorage
function loadPref(key, fallback) {
  try { const v = localStorage.getItem('prview.' + key); return v === null ? fallback : v === 'true'; }
  catch { return fallback; }
}
function savePref(key, val) {
  try { localStorage.setItem('prview.' + key, val); } catch {}
}

let activeTab = (() => {
  try {
    const v = localStorage.getItem('prview.activeTab');
    if (v === 'true') return 'reviews';  // backward compat
    if (v === 'false' || v === null) return 'my-prs';
    return v;
  } catch { return 'my-prs'; }
})();
let showHidden = loadPref('showHidden', false);
let showDrafts = loadPref('showDrafts', false);
let showApproved = loadPref('showApproved', false);
let showRejected = loadPref('showRejected', false);
let showRead = loadPref('showRead', false);

// --- Sort state ---
function loadSortPref(key, defaultCol, defaultDir) {
  try {
    const v = getCookie('prview_sort_' + key);
    if (v) { const [col, dir] = v.split(':'); return { col, dir }; }
  } catch {}
  return { col: defaultCol, dir: defaultDir };
}
function saveSortPref(key, col, dir) {
  setCookie('prview_sort_' + key, col + ':' + dir);
}

let myPrsSort = loadSortPref('my', 'updated_at', 'desc');
let reviewsSort = loadSortPref('reviews', 'updated_at', 'desc');
let issuesSort = loadSortPref('issues', 'updated_at', 'desc');

const myPrsCols = [
  { key: null, label: '', cls: 'col-checkbox' },
  { key: 'repo', label: 'Repo' },
  { key: 'number', label: 'PR' },
  { key: 'title', label: 'Title' },
  { key: 'review_status', label: 'Review' },
  { key: 'checks_overall', label: 'CI' },
  { key: 'drci_emoji', label: 'DrCI' },
  { key: 'comment_count', label: 'Comments', narrowLabel: 'C' },
  { key: 'updated_at', label: 'Updated', narrowLabel: 'U' },
];
const reviewsCols = [
  { key: 'repo', label: 'Repo' },
  { key: 'number', label: 'PR' },
  { key: 'title', label: 'Title' },
  { key: 'author', label: 'Author' },
  { key: 'review_status', label: 'Review' },
  { key: 'checks_overall', label: 'CI' },
  { key: 'drci_emoji', label: 'DrCI' },
  { key: 'comment_count', label: 'Comments', narrowLabel: 'C' },
  { key: 'updated_at', label: 'Updated', narrowLabel: 'U' },
  { key: null, label: '', cls: 'col-menu' },
];
const issuesCols = [
  { key: 'repo', label: 'Repo' },
  { key: 'number', label: 'Issue' },
  { key: 'title', label: 'Title' },
  { key: 'author', label: 'Author' },
  { key: null, label: 'Labels' },
  { key: 'comment_count', label: 'Comments', narrowLabel: 'C' },
  { key: 'updated_at', label: 'Updated', narrowLabel: 'U' },
];

function renderHeaders(theadId, cols, sortState, onSort) {
  const thead = document.getElementById(theadId);
  thead.innerHTML = '<tr>' + cols.map(c => {
    if (!c.key) return `<th${c.cls ? ' class="' + c.cls + '"' : ''}></th>`;
    const active = sortState.col === c.key;
    const arrow = active ? (sortState.dir === 'asc' ? '\u25b2' : '\u25bc') : '\u25b4';
    const cls = 'sortable' + (active ? ' sort-active' : '');
    const display = (isMedium && c.narrowLabel) ? c.narrowLabel : c.label;
    return `<th class="${cls}" data-sort="${c.key}" title="${c.label}">${display}<span class="sort-arrow">${arrow}</span></th>`;
  }).join('') + '</tr>';
  thead.querySelectorAll('th.sortable').forEach(th => {
    th.onclick = () => onSort(th.dataset.sort);
  });
}

function genericCompare(a, b, col) {
  let va = a[col], vb = b[col];
  if (va == null) va = '';
  if (vb == null) vb = '';
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  if (typeof va === 'boolean' && typeof vb === 'boolean') return (va ? 1 : 0) - (vb ? 1 : 0);
  return String(va).localeCompare(String(vb));
}

function sortList(list, sortState) {
  const dir = sortState.dir === 'asc' ? 1 : -1;
  list.sort((a, b) => dir * genericCompare(a, b, sortState.col));
}

// --- Tabs ---
function activateTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${id}"]`).classList.add('active');
  document.getElementById(id + '-panel').classList.add('active');
  activeTab = id;
  savePref('activeTab', id);
}
document.querySelectorAll('.tab').forEach(tab => {
  tab.onclick = () => activateTab(tab.dataset.tab);
});
activateTab(activeTab);

// --- Shared helpers ---
function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function relativeTime(iso) {
  if (!iso) return '';
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diff = Math.max(0, now - then);
  const mins = Math.floor(diff / 60000);
  const ago = (isNarrow || isMedium) ? '' : ' ago';
  if (mins < 1) return (isNarrow || isMedium) ? 'now' : 'just now';
  if (mins < 60) return mins + 'm' + ago;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return hrs + 'h' + ago;
  const days = Math.floor(hrs / 24);
  if (days < 30) return days + 'd' + ago;
  const months = Math.floor(days / 30);
  return months + 'mo' + ago;
}

function reviewPill(pr) {
  let reviewers;
  try { reviewers = JSON.parse(pr.reviewers); } catch { reviewers = []; }
  const tip = reviewers.length === 0
    ? 'No reviews yet'
    : reviewers.map(r => r.login + ': ' + r.state).join('\n');
  if (pr.review_status === 'APPROVED')
    return `<span class="pill pill-green" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Approved</span>`;
  if (pr.review_status === 'CHANGES_REQUESTED')
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Changes</span>`;
  if (reviewers.length > 0)
    return `<span class="pill pill-yellow" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Pending</span>`;
  return `<span class="pill pill-muted" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>None</span>`;
}

function drciPill(pr) {
  if (!pr.drci_emoji) {
    if (pr.checks_pending > 0)
      return '<span class="pill pill-yellow" data-tip="DrCI pending"><span class="spinner"></span>Pending</span>';
    return '<span class="pill pill-muted" data-tip="No DrCI status"><span class="pill-dot"></span>None</span>';
  }
  const tip = escapeHtml(pr.drci_status);
  const m = {
    'white_check_mark': ['pill-green', 'Passing'],
    'x': ['pill-red', pr.checks_pending > 0 ? 'Failing' : 'Failed'],
    'hourglass_flowing_sand': ['pill-yellow', 'Running'],
  };
  const [cls, label] = m[pr.drci_emoji] || ['pill-muted', 'Unknown'];
  const spinning = cls === 'pill-yellow' || (pr.checks_pending > 0);
  const dot = spinning
    ? `<span class="spinner${cls === 'pill-red' ? ' spinner-red' : ''}"></span>`
    : '<span class="pill-dot"></span>';
  return `<span class="pill ${cls}" data-tip="${tip}">${dot}${label}</span>`;
}


function checksOverallPill(pr) {
  if (!pr.checks_overall) return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  if (pr.checks_overall === 'SUCCESS')
    return '<span class="pill pill-green" data-tip="Checks passed"><span class="pill-dot"></span>Passing</span>';
  if (pr.checks_overall === 'FAILURE' || pr.checks_overall === 'ERROR') {
    if (pr.checks_running)
      return '<span class="pill pill-red" data-tip="Checks failing, still running"><span class="spinner spinner-red"></span>Failed</span>';
    return '<span class="pill pill-red" data-tip="Checks failed"><span class="pill-dot"></span>Failed</span>';
  }
  return '<span class="pill pill-yellow" data-tip="Checks pending"><span class="spinner"></span>Pending</span>';
}

function detailedCIPill(pr) {
  const total = (pr.checks_success || 0) + (pr.checks_fail || 0) + (pr.checks_pending || 0);
  if (total === 0) return checksOverallPill(pr);
  const tip = `${pr.checks_success} passed, ${pr.checks_fail} failed, ${pr.checks_pending} pending`;
  if (pr.checks_fail > 0) {
    const dot = pr.checks_running
      ? '<span class="spinner spinner-red"></span>'
      : '<span class="pill-dot"></span>';
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}">${dot}${pr.checks_fail} failed</span>`;
  }
  if (pr.checks_pending > 0)
    return `<span class="pill pill-yellow" data-tip="${escapeHtml(tip)}"><span class="spinner"></span>${pr.checks_pending} pending</span>`;
  return `<span class="pill pill-green" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Passing</span>`;
}

function ciOrLandingPill(pr) {
  const total = (pr.checks_success || 0) + (pr.checks_fail || 0) + (pr.checks_pending || 0);
  const tip = total > 0
    ? escapeHtml(`${pr.checks_success} passed, ${pr.checks_fail} failed, ${pr.checks_pending} pending`)
    : '';
  if (pr.landing_status === 'landing')
    return `<span class="pill pill-yellow" data-tip="${tip}"><span class="spinner"></span>Landing</span>`;
  if (pr.landing_status === 'reverted')
    return `<span class="pill pill-red" data-tip="${tip}"><span class="pill-dot"></span>Reverted</span>`;
  if (pr.landing_status === 'failed')
    return `<span class="pill pill-red" data-tip="${tip}"><span class="pill-dot"></span>Land Failed</span>`;
  return detailedCIPill(pr);
}

function reviewCombinedPill(pr) {
  let reviewers;
  try { reviewers = JSON.parse(pr.reviewers); } catch { reviewers = []; }
  const revTip = reviewers.length === 0
    ? 'No reviews yet'
    : reviewers.map(r => r.login + ': ' + r.state).join('\n');
  if (pr.is_mentioned) {
    return `<span class="pill pill-mention" data-tip="${escapeHtml('You were mentioned in a comment')}"><span class="pill-dot"></span>Mention</span>`;
  }
  if (pr.ci_approval_needed) {
    const tip = 'CI requires approval\n' + revTip;
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}"><span class="spinner spinner-red"></span>CI Approval</span>`;
  }
  if (pr.is_draft)
    return `<span class="pill pill-muted" data-tip="${escapeHtml('Draft PR\n' + revTip)}">Draft</span>`;
  const tip = revTip;
  if (pr.review_status === 'CHANGES_REQUESTED')
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Changes</span>`;
  if (pr.review_status === 'APPROVED')
    return `<span class="pill pill-green" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Approved</span>`;
  if (reviewers.length > 0)
    return `<span class="pill pill-yellow" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Pending</span>`;
  return `<span class="pill pill-muted" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>None</span>`;
}

function commentCell(pr) {
  if (pr.comment_count > 0) return `<span class="comment-count">${pr.comment_count}</span>`;
  return '';
}

function prKey(pr) { return pr.repo + '#' + pr.number; }

// --- My PRs tab ---
function toggleMyPrsSort(col) {
  if (myPrsSort.col === col) myPrsSort.dir = myPrsSort.dir === 'asc' ? 'desc' : 'asc';
  else { myPrsSort.col = col; myPrsSort.dir = 'desc'; }
  saveSortPref('my', myPrsSort.col, myPrsSort.dir);
  sortList(allPrs, myPrsSort);
  renderMyPrs();
}

function prRow(pr) {
  let cls = [];
  if (pr.hidden) cls.push('hidden-row');
  if (pr.is_draft) cls.push('draft-row');
  const rowClass = cls.length ? ` class="${cls.join(' ')}"` : '';
  const checked = pr.hidden ? ' checked' : '';
  return `<tr${rowClass} data-key="${escapeHtml(prKey(pr))}">
    <td><input type="checkbox"${checked} onchange="toggleHidden('${escapeHtml(pr.repo)}', ${pr.number}, this.checked)" title="Hide this PR"></td>
    <td class="repo-cell"><span class="repo-text">${escapeHtml(pr.repo)}</span></td>
    <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank">#${pr.number}</a></td>
    <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank">${escapeHtml(pr.title)}</a></td>
    <td>${pr.is_draft ? '<span class="pill pill-muted">Draft</span>' : reviewPill(pr)}</td>
    <td>${ciOrLandingPill(pr)}</td>
    <td>${drciPill(pr)}</td>
    <td>${commentCell(pr)}</td>
    <td><span class="time-text" title="${escapeHtml(pr.updated_at)}">${relativeTime(pr.updated_at)}</span></td>
  </tr>`;
}

let draftsOpen = loadPref('draftsOpen', true);

function toggleDrafts() {
  draftsOpen = !draftsOpen;
  savePref('draftsOpen', draftsOpen);
  renderMyPrs();
}

function renderMyPrs() {
  renderHeaders('my-prs-thead', myPrsCols, myPrsSort, toggleMyPrsSort);
  const all = showHidden ? allPrs : allPrs.filter(p => !p.hidden);
  const open = all.filter(p => !p.is_draft);
  const drafts = all.filter(p => p.is_draft);
  const tbody = document.getElementById('my-prs-body');
  const bar = document.getElementById('my-prs-filter-bar');

  if (hiddenCount > 0) {
    bar.innerHTML = `<button class="chip${showHidden ? ' active' : ''}" id="hide-toggle">
      ${showHidden ? 'Showing' : 'Show'} ${hiddenCount} hidden <span class="chip-count"></span>
    </button>`;
    document.getElementById('hide-toggle').onclick = () => {
      showHidden = !showHidden;
      savePref('showHidden', showHidden);
      renderMyPrs();
    };
  } else {
    bar.innerHTML = '';
  }

  const nonHidden = allPrs.filter(p => !p.hidden);
  const nonDraftCount = nonHidden.filter(p => !p.is_draft).length;
  const draftCount2 = nonHidden.filter(p => p.is_draft).length;
  document.getElementById('my-prs-count').textContent = nonDraftCount;
  const draftCountEl = document.getElementById('my-prs-draft-count');
  if (draftCount2 > 0) {
    draftCountEl.textContent = '+' + draftCount2;
    draftCountEl.style.display = '';
  } else {
    draftCountEl.style.display = 'none';
  }

  if (open.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state">' +
      (hasFetched ? 'No open PRs' : 'Fetching...') + '</td></tr>';
  } else {
    tbody.innerHTML = open.map(prRow).join('');
  }

  // Drafts section
  const draftsSection = document.getElementById('drafts-section');
  const draftsArrow = document.getElementById('drafts-arrow');
  const draftsBody = document.getElementById('drafts-body');
  document.getElementById('drafts-count').textContent = drafts.length;

  if (drafts.length === 0) {
    draftsSection.style.display = 'none';
  } else {
    draftsSection.style.display = '';
    if (draftsOpen) {
      draftsArrow.classList.add('open');
      draftsBody.style.display = '';
      renderHeaders('drafts-thead', myPrsCols, myPrsSort, toggleMyPrsSort);
      document.getElementById('drafts-tbody').innerHTML = drafts.map(prRow).join('');
    } else {
      draftsArrow.classList.remove('open');
      draftsBody.style.display = 'none';
    }
  }
}

// --- Recently Landed ---
let landedOpen = loadPref('landedOpen', false);

function toggleLanded() {
  landedOpen = !landedOpen;
  savePref('landedOpen', landedOpen);
  renderLanded();
}

function renderLanded() {
  const section = document.getElementById('landed-section');
  const body = document.getElementById('landed-body');
  const arrow = document.getElementById('landed-arrow');
  const count = document.getElementById('landed-count');

  if (allMergedPrs.length === 0) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';
  count.textContent = allMergedPrs.length;

  if (landedOpen) {
    arrow.classList.add('open');
    body.style.display = '';
    const tbody = document.getElementById('landed-tbody');
    tbody.innerHTML = allMergedPrs.map(pr => {
      const repo = escapeHtml(pr.repo);
      const shortRepo = repo.split('/').pop();
      return `<tr>
        <td class="repo-cell"><span class="repo-text">${shortRepo}</span></td>
        <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank">#${pr.number}</a></td>
        <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank">${escapeHtml(pr.title)}</a></td>
        <td><span class="time-text" title="${escapeHtml(pr.landed_at)}">${relativeTime(pr.landed_at)}</span></td>
      </tr>`;
    }).join('');
  } else {
    arrow.classList.remove('open');
    body.style.display = 'none';
  }
}

// --- Reviews tab ---
function toggleReviewsSort(col) {
  if (reviewsSort.col === col) reviewsSort.dir = reviewsSort.dir === 'asc' ? 'desc' : 'asc';
  else { reviewsSort.col = col; reviewsSort.dir = 'desc'; }
  saveSortPref('reviews', reviewsSort.col, reviewsSort.dir);
  sortList(allReviewPrs, reviewsSort);
  renderReviews();
}

function renderReviews() {
  renderHeaders('reviews-thead', reviewsCols, reviewsSort, toggleReviewsSort);
  let visible = allReviewPrs;
  if (!showDrafts) visible = visible.filter(p => !p.is_draft);
  if (!showApproved) visible = visible.filter(p => p.review_status !== 'APPROVED');
  if (!showRejected) visible = visible.filter(p => p.review_status !== 'CHANGES_REQUESTED');

  // Count read items before applying read filter (for tab badge and chip)
  const unreadCount = visible.filter(p => !p.is_read).length;
  const readCount = visible.length - unreadCount;
  if (!showRead) visible = visible.filter(p => !p.is_read);

  const tbody = document.getElementById('reviews-body');
  const bar = document.getElementById('reviews-filter-bar');

  const draftCount = allReviewPrs.filter(p => p.is_draft).length;
  const approvedCount = allReviewPrs.filter(p => p.review_status === 'APPROVED').length;
  const rejectedCount = allReviewPrs.filter(p => p.review_status === 'CHANGES_REQUESTED').length;

  let chips = [];
  if (draftCount > 0) {
    chips.push(`<button class="chip${showDrafts ? ' active' : ''}" id="draft-toggle">
      ${showDrafts ? 'Showing' : 'Show'} ${draftCount} drafts</button>`);
  }
  if (approvedCount > 0) {
    chips.push(`<button class="chip${showApproved ? ' active' : ''}" id="approved-toggle">
      ${showApproved ? 'Showing' : 'Show'} ${approvedCount} approved</button>`);
  }
  if (rejectedCount > 0) {
    chips.push(`<button class="chip${showRejected ? ' active' : ''}" id="rejected-toggle">
      ${showRejected ? 'Showing' : 'Show'} ${rejectedCount} changes requested</button>`);
  }
  if (readCount > 0) {
    chips.push(`<button class="chip${showRead ? ' active' : ''}" id="read-toggle">
      ${showRead ? 'Showing' : 'Show'} ${readCount} read</button>`);
  }
  bar.innerHTML = chips.join('');

  if (draftCount > 0) {
    document.getElementById('draft-toggle').onclick = () => {
      showDrafts = !showDrafts;
      savePref('showDrafts', showDrafts);
      renderReviews();
    };
  }
  if (approvedCount > 0) {
    document.getElementById('approved-toggle').onclick = () => {
      showApproved = !showApproved;
      savePref('showApproved', showApproved);
      renderReviews();
    };
  }
  if (rejectedCount > 0) {
    document.getElementById('rejected-toggle').onclick = () => {
      showRejected = !showRejected;
      savePref('showRejected', showRejected);
      renderReviews();
    };
  }
  if (readCount > 0) {
    document.getElementById('read-toggle').onclick = () => {
      showRead = !showRead;
      savePref('showRead', showRead);
      renderReviews();
    };
  }

  // Update count + attention pulse
  const countEl = document.getElementById('reviews-count');
  countEl.textContent = unreadCount;
  countEl.classList.toggle('attention', unreadCount > 5);

  if (visible.length === 0) {
    tbody.innerHTML = '<tr><td colspan="10" class="empty-state">' +
      (hasFetched ? 'No review requests' : 'Fetching...') + '</td></tr>';
    return;
  }

  function reviewRow(pr) {
    let cls = [];
    if (pr.is_draft) cls.push('draft-row');
    if (pr.is_read) cls.push('read-row');
    const rowClass = cls.length ? ` class="${cls.join(' ')}"` : '';
    const menuLabel = pr.is_read ? 'Mark unread' : 'Mark read';
    const menuRead = pr.is_read ? 'false' : 'true';
    const mentionLabel = pr.is_mentioned ? 'Clear mention' : 'Mark mention';
    const mentionVal = pr.is_mentioned ? 'false' : 'true';
    return `<tr${rowClass} data-key="${escapeHtml(prKey(pr))}">
      <td class="repo-cell"><span class="repo-text">${escapeHtml(pr.repo)}</span></td>
      <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank" onclick="markRead('${escapeHtml(pr.repo)}', ${pr.number})">#${pr.number}</a></td>
      <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank" onclick="markRead('${escapeHtml(pr.repo)}', ${pr.number})">${escapeHtml(pr.title)}</a></td>
      <td class="author-cell"><span class="author-text">${escapeHtml(pr.author)}</span></td>
      <td>${reviewCombinedPill(pr)}</td>
      <td>${detailedCIPill(pr)}</td>
      <td>${drciPill(pr)}</td>
      <td>${commentCell(pr)}</td>
      <td><span class="time-text" title="${escapeHtml(pr.updated_at || '')}">${relativeTime(pr.updated_at)}</span></td>
      <td class="menu-cell">
        <button class="menu-btn" onclick="toggleMenu(event)">&#x22ef;</button>
        <div class="dropdown">
          <a href="${escapeHtml(pr.url)}" target="_blank" onclick="event.stopPropagation(); closeAllMenus();">Open unread</a>
          <a href="#" onclick="setReviewRead(event, '${escapeHtml(pr.repo)}', ${pr.number}, ${menuRead})">${menuLabel}</a>
          <a href="#" onclick="setReviewMention(event, '${escapeHtml(pr.repo)}', ${pr.number}, ${mentionVal})">${mentionLabel}</a>
        </div>
      </td>
    </tr>`;
  }
  tbody.innerHTML = visible.map(reviewRow).join('');
}

// --- Issues tab ---
function toggleIssuesSort(col) {
  if (issuesSort.col === col) issuesSort.dir = issuesSort.dir === 'asc' ? 'desc' : 'asc';
  else { issuesSort.col = col; issuesSort.dir = 'desc'; }
  saveSortPref('issues', issuesSort.col, issuesSort.dir);
  sortList(allIssues, issuesSort);
  renderIssues();
}

function issueKey(i) { return i.repo + '#' + i.number; }

function labelPills(labelsJson) {
  let labels;
  try { labels = JSON.parse(labelsJson); } catch { return ''; }
  if (!labels || labels.length === 0) return '';
  return labels.map(l => {
    const bg = '#' + l.color;
    // Compute luminance to pick text color
    const r = parseInt(l.color.substr(0, 2), 16) / 255;
    const g = parseInt(l.color.substr(2, 2), 16) / 255;
    const b = parseInt(l.color.substr(4, 2), 16) / 255;
    const lum = 0.299 * r + 0.587 * g + 0.114 * b;
    const fg = lum > 0.5 ? '#000' : '#fff';
    return `<span class="label-pill" style="background:${bg};color:${fg}">${escapeHtml(l.name)}</span>`;
  }).join('');
}

function renderIssues() {
  renderHeaders('issues-thead', issuesCols, issuesSort, toggleIssuesSort);
  const tbody = document.getElementById('issues-body');
  document.getElementById('issues-count').textContent = allIssues.length;

  if (allIssues.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state">' +
      (hasFetched ? 'No assigned issues' : 'Fetching...') + '</td></tr>';
    return;
  }

  tbody.innerHTML = allIssues.map(issue => {
    return `<tr data-key="${escapeHtml(issueKey(issue))}">
      <td class="repo-cell"><span class="repo-text">${escapeHtml(issue.repo)}</span></td>
      <td class="mono"><a href="${escapeHtml(issue.url)}" target="_blank">#${issue.number}</a></td>
      <td class="title-cell"><a href="${escapeHtml(issue.url)}" target="_blank">${escapeHtml(issue.title)}</a></td>
      <td class="author-cell"><span class="author-text">${escapeHtml(issue.author)}</span></td>
      <td class="labels-cell">${labelPills(issue.labels)}</td>
      <td>${issue.comment_count > 0 ? '<span class="comment-count">' + issue.comment_count + '</span>' : ''}</td>
      <td><span class="time-text" title="${escapeHtml(issue.updated_at)}">${relativeTime(issue.updated_at)}</span></td>
    </tr>`;
  }).join('');
}

function renderAll() {
  renderMyPrs();
  renderLanded();
  renderReviews();
  renderIssues();
}

// --- Updates ---
function applyUpdate(batch) {
  const fetchErr = document.getElementById('fetch-error');
  if (batch.error) {
    fetchErr.textContent = 'Fetch error: ' + batch.error;
    fetchErr.style.display = 'block';
    return;
  }
  fetchErr.style.display = 'none';
  hasFetched = true;

  hiddenCount = batch.hidden_count;

  for (const u of batch.pr_updates) {
    if (u.type === 'changed') {
      const key = prKey(u);
      const idx = allPrs.findIndex(p => prKey(p) === key);
      if (idx >= 0) allPrs[idx] = u;
      else allPrs.push(u);
    } else if (u.type === 'removed') {
      const key = u.repo + '#' + u.number;
      allPrs = allPrs.filter(p => prKey(p) !== key);
    }
  }
  sortList(allPrs, myPrsSort);

  for (const u of batch.review_updates) {
    if (u.type === 'changed') {
      const key = prKey(u);
      const idx = allReviewPrs.findIndex(p => prKey(p) === key);
      if (idx >= 0) allReviewPrs[idx] = u;
      else allReviewPrs.push(u);
    } else if (u.type === 'removed') {
      const key = u.repo + '#' + u.number;
      allReviewPrs = allReviewPrs.filter(p => prKey(p) !== key);
    }
  }
  sortList(allReviewPrs, reviewsSort);

  if (batch.merged_prs && batch.merged_prs.length > 0) {
    allMergedPrs = batch.merged_prs;
  }

  if (batch.issue_updates) {
    for (const u of batch.issue_updates) {
      if (u.type === 'changed') {
        const key = issueKey(u);
        const idx = allIssues.findIndex(i => issueKey(i) === key);
        if (idx >= 0) allIssues[idx] = u;
        else allIssues.push(u);
      } else if (u.type === 'removed') {
        const key = u.repo + '#' + u.number;
        allIssues = allIssues.filter(i => issueKey(i) !== key);
      }
    }
    sortList(allIssues, issuesSort);
  }

  renderAll();
}

function toggleHidden(repo, number, hidden) {
  fetch('/api/toggle-hidden', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: currentUser, repo, number, hidden}),
  });
  const key = repo + '#' + number;
  const pr = allPrs.find(p => prKey(p) === key);
  if (pr) {
    pr.hidden = hidden;
    hiddenCount += hidden ? 1 : -1;
    renderAll();
  }
}

function closeAllMenus() {
  document.querySelectorAll('.dropdown.open').forEach(d => {
    d.classList.remove('open');
    d.style.cssText = '';
  });
}

function toggleMenu(e) {
  e.stopPropagation();
  const btn = e.currentTarget;
  const dd = btn.nextElementSibling;
  const wasOpen = dd.classList.contains('open');
  closeAllMenus();
  if (wasOpen) return;
  dd.classList.add('open');
  // Use position:fixed so the dropdown escapes any overflow:hidden ancestors
  // (notably .card). Pick above-or-below based on which half of the viewport
  // the button is in, so it stays on screen near the top and bottom edges.
  const r = btn.getBoundingClientRect();
  const right = Math.max(0, window.innerWidth - r.right);
  if (r.top < window.innerHeight / 2) {
    dd.style.cssText = `position: fixed; right: ${right}px; top: ${r.bottom}px; bottom: auto;`;
  } else {
    dd.style.cssText = `position: fixed; right: ${right}px; bottom: ${window.innerHeight - r.top}px; top: auto;`;
  }
}

document.addEventListener('click', closeAllMenus);
window.addEventListener('scroll', closeAllMenus, true);
window.addEventListener('resize', closeAllMenus);

function setReviewRead(e, repo, number, read) {
  e.preventDefault();
  e.stopPropagation();
  closeAllMenus();
  fetch('/api/toggle-review-read', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: currentUser, repo, number, read}),
  });
  const key = repo + '#' + number;
  const pr = allReviewPrs.find(p => prKey(p) === key);
  if (pr) {
    pr.is_read = read;
    if (read) pr.is_mentioned = false;
    renderReviews();
  }
}

function setReviewMention(e, repo, number, mentioned) {
  e.preventDefault();
  e.stopPropagation();
  closeAllMenus();
  fetch('/api/toggle-mention', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: currentUser, repo, number, mentioned}),
  });
  const key = repo + '#' + number;
  const pr = allReviewPrs.find(p => prKey(p) === key);
  if (pr) {
    pr.is_mentioned = mentioned;
    renderReviews();
  }
}

function markRead(repo, number) {
  const key = repo + '#' + number;
  const pr = allReviewPrs.find(p => prKey(p) === key);
  if (pr && !pr.is_read) {
    fetch('/api/toggle-review-read', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({user: currentUser, repo, number, read: true}),
    });
    pr.is_read = true;
    pr.is_mentioned = false;
    renderReviews();
  }
}

let currentUser = '';

// Cookie helpers
function setCookie(name, val) {
  document.cookie = name + '=' + encodeURIComponent(val) + ';path=/;max-age=31536000;SameSite=Lax';
}
function getCookie(name) {
  const m = document.cookie.match('(?:^|; )' + name + '=([^;]*)');
  return m ? decodeURIComponent(m[1]) : '';
}

function setUser(user) {
  user = user.trim();
  currentUser = user;
  setCookie('prview_user', user);
  const meBtn = document.getElementById('user-me-btn');
  const input = document.getElementById('user-input');
  if (user === '') {
    meBtn.classList.add('active');
    input.classList.remove('active-user');
    input.value = '';
  } else {
    meBtn.classList.remove('active');
    input.classList.add('active-user');
    input.value = user;
  }
  allPrs = [];
  allReviewPrs = [];
  allMergedPrs = [];
  allIssues = [];
  hiddenCount = 0;
  hasFetched = false;
  renderAll();
  fetch('/api/set-user', { method: 'POST' });
  connectSSE();
}

function refreshNow() {
  const btn = document.getElementById('refresh-btn');
  btn.classList.add('spinning');
  fetch('/api/refresh', { method: 'POST' })
    .finally(() => setTimeout(() => btn.classList.remove('spinning'), 1000));
}

// --- SSE ---
let evtSource = null;

function checkBuildHash(hash) {
  if (buildHash === null) {
    buildHash = hash;
  } else if (hash !== buildHash) {
    location.reload();
  }
}

function connectSSE() {
  if (evtSource) evtSource.close();
  const params = currentUser ? '?user=' + encodeURIComponent(currentUser) : '';
  evtSource = new EventSource('/api/events' + params);

  evtSource.addEventListener('init', (e) => {
    document.getElementById('conn-error').style.display = 'none';
    const data = JSON.parse(e.data);
    checkBuildHash(data.build_hash);
    allPrs = data.prs;
    sortList(allPrs, myPrsSort);
    allReviewPrs = data.review_prs;
    sortList(allReviewPrs, reviewsSort);
    allMergedPrs = data.merged_prs || [];
    allIssues = data.issues || [];
    sortList(allIssues, issuesSort);
    hiddenCount = data.hidden_count;
    hasFetched = (allPrs.length > 0 || allReviewPrs.length > 0 || allIssues.length > 0);
    renderAll();
  });

  evtSource.addEventListener('update', (e) => {
    document.getElementById('conn-error').style.display = 'none';
    const batch = JSON.parse(e.data);
    checkBuildHash(batch.build_hash);
    applyUpdate(batch);
  });

  evtSource.onerror = () => {
    document.getElementById('conn-error').style.display = 'block';
  };
}

// Restore user from cookie
const savedUser = getCookie('prview_user');
if (savedUser) {
  currentUser = savedUser;
  const meBtn = document.getElementById('user-me-btn');
  const input = document.getElementById('user-input');
  meBtn.classList.remove('active');
  input.classList.add('active-user');
  input.value = savedUser;
}

connectSSE();
