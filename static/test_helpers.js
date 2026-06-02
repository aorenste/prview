// Shared helpers for UI tests. Not picked up by `node --test` because the
// filename doesn't match `*.test.js`.

const { JSDOM } = require('jsdom');
const fs = require('node:fs');
const path = require('node:path');

function loadApp(opts = {}) {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const css = fs.readFileSync(path.join(__dirname, 'style.css'), 'utf8');
  // Inline the stylesheet so jsdom applies it without fetching.
  const htmlWithCss = html.replace(
    '<link rel="stylesheet" href="/static/style.css">',
    `<style>${css}</style>`
  );
  // A url is required for jsdom to expose window.localStorage (app.js prefs).
  const dom = new JSDOM(htmlWithCss, {
    runScripts: 'outside-only',
    pretendToBeVisual: true,
    url: 'https://localhost/',
  });
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

  // Seed localStorage prefs before app.js reads them on load (loadPref keys
  // are stored as 'prview.<key>').
  if (opts.prefs) {
    for (const [key, val] of Object.entries(opts.prefs)) {
      win.localStorage.setItem('prview.' + key, String(val));
    }
  }

  const js = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');
  win.eval(js);
  return win;
}

module.exports = { loadApp };
