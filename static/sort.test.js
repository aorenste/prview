// jsdom-based UI test for the column-sort flow.
// Loads the real app.js into a simulated DOM and dispatches a real click
// on a sortable header. This catches things like ReferenceError in the
// click handler that pure-logic tests can't see.

const { test } = require('node:test');
const assert = require('node:assert');
const { JSDOM } = require('jsdom');
const fs = require('node:fs');
const path = require('node:path');

function loadApp() {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const dom = new JSDOM(html, { runScripts: 'outside-only', pretendToBeVisual: true });
  const win = dom.window;

  // Stub the browser APIs app.js touches on load so the script can finish.
  win.EventSource = class {
    constructor() {}
    addEventListener() {}
    close() {}
    set onerror(_) {}
  };
  win.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  win.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });

  // Capture any uncaught error from event handlers — jsdom routes those to
  // window.onerror rather than letting them propagate out of click().
  win.__errors = [];
  win.addEventListener('error', (e) => {
    win.__errors.push(e.error || new Error(e.message));
  });

  const js = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');
  win.eval(js);
  return win;
}

test('clicking a sortable column header marks it active', () => {
  const win = loadApp();
  // Trigger the initial render so the thead populates with sortable cells.
  // (Normally driven by the SSE init event, which we don't simulate here.)
  win.renderAll();

  const repoTh = win.document.querySelector('#my-prs-thead th[data-sort="repo"]');
  assert.ok(repoTh, 'expected a sortable repo header to exist');
  assert.ok(!repoTh.classList.contains('sort-active'),
    'repo should not be the active sort initially');

  repoTh.click();

  // The handler re-renders the thead, so re-query.
  const repoThAfter = win.document.querySelector('#my-prs-thead th[data-sort="repo"]');
  assert.equal(win.__errors.length, 0,
    `click threw: ${win.__errors.map(e => e.message).join('; ')}`);
  assert.ok(repoThAfter.classList.contains('sort-active'),
    'repo should be the active sort after clicking its header');
});
