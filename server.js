// Space backend — zero dependencies (Node 20+).
// Google sign-in, per-user accounts, and a daily Apify sync that turns
// per-video view counts into "views gained per day" and "clips posted per day".
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

/* ---------- config (.env) ---------- */
const ROOT = __dirname;
try {
  for (const line of fs.readFileSync(path.join(ROOT, '.env'), 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !line.trim().startsWith('#') && process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
} catch {}
const PORT = +process.env.PORT || 8765;
const BASE_URL = (process.env.BASE_URL || process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`).replace(/\/$/, '');
const hosted = !/^https?:\/\/(localhost|127\.0\.0\.1)(:|$)/.test(BASE_URL);
const ALLOWED = (process.env.ALLOWED_EMAILS || '').toLowerCase().split(',').map(s => s.trim()).filter(Boolean);
const KV_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const KV_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const GOOGLE_ID = process.env.GOOGLE_CLIENT_ID || '';
const GOOGLE_SECRET = process.env.GOOGLE_CLIENT_SECRET || '';
const APIFY_TOKEN = process.env.APIFY_TOKEN || '';
const SYNC_HOURS = +process.env.SYNC_HOURS || 24;
const FIRST_DAYS = +process.env.FIRST_DAYS || 2;      // new account: clips posted in the last N days
const DAILY_HOURS = +process.env.DAILY_HOURS || 30;   // daily check: clips posted in the last N hours (24h + margin)
const LIMIT = 40;                                     // safety cap per check
const googleOn = !!(GOOGLE_ID && GOOGLE_SECRET);
const secure = BASE_URL.startsWith('https://');

/* ---------- storage (one JSON file) ---------- */
const DB_FILE = process.env.DB_FILE || path.join(ROOT, 'data', 'db.json');
let db = { users: {}, sessions: {} };
// Online (Upstash Redis, one key) when UPSTASH_* is set; otherwise a local file.
const kv = (cmd, body) => fetch(`${KV_URL}/${cmd}`, { method: body === undefined ? 'GET' : 'POST', headers: { Authorization: `Bearer ${KV_TOKEN}` }, body });
async function loadDb() {
  if (KV_URL) {
    const r = await kv('get/space:db');
    if (!r.ok) throw new Error(`Upstash refused the connection (${r.status})`); // never start empty and overwrite real data
    const j = await r.json();
    if (j.result) db = JSON.parse(j.result);
  } else { try { db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch {} }
}
let saveTimer;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    if (KV_URL) return kv('set/space:db', JSON.stringify(db)).then(r => { if (!r.ok) console.error('Save failed', r.status); }).catch(e => console.error('Save failed', e.message));
    fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
    fs.writeFileSync(DB_FILE + '.tmp', JSON.stringify(db));
    fs.renameSync(DB_FILE + '.tmp', DB_FILE);
  }, 150);
}

/* ---------- dates ---------- */
const iso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const today = () => iso(new Date());

/* ---------- Apify ---------- */
const clean = h => String(h).trim().replace(/^(https?:\/\/)?(www\.)?(tiktok|instagram|youtube)\.com\//i, '').replace(/^@/, '').replace(/[/?#].*$/, '');
const ACTORS = {
  TikTok: {
    actor: 'clockworks~tiktok-scraper',
    input: (h, since) => ({ profiles: [h], resultsPerPage: LIMIT, profileScrapeSections: ['videos'], profileSorting: 'latest', shouldDownloadVideos: false, shouldDownloadCovers: false, oldestPostDateUnified: since }),
    map: i => ({ id: i.id, views: i.playCount, date: i.createTimeISO || (i.createTime ? i.createTime * 1000 : null) }),
  },
  Instagram: {
    actor: 'apify~instagram-reel-scraper',
    input: (h, since) => ({ username: [h], resultsLimit: LIMIT, onlyPostsNewerThan: since }),
    map: i => ({ id: i.shortCode || i.id, views: i.videoPlayCount ?? i.videoViewCount, date: i.timestamp }),
  },
  YouTube: {
    actor: 'streamers~youtube-shorts-scraper',
    input: (h, since) => ({ channels: [h], maxResultsShorts: LIMIT, oldestPostDate: since }),
    map: i => ({ id: i.id, views: i.viewCount, date: i.date }),
  },
};
async function apify(url, opts = {}) {
  const r = await fetch('https://api.apify.com/v2' + url, { ...opts, headers: { Authorization: 'Bearer ' + APIFY_TOKEN, 'Content-Type': 'application/json' } });
  const body = await r.json().catch(() => null);
  if (!r.ok) throw new Error(body?.error?.message || `Apify error ${r.status}`);
  return body;
}
async function scrape(platform, handle, first) {
  const a = ACTORS[platform];
  const cutoff = Date.now() - (first ? FIRST_DAYS * 24 : DAILY_HOURS) * 36e5;
  const since = new Date(cutoff - 864e5).toISOString().slice(0, 10); // day-level filter for the scraper, exact cut below
  let run = (await apify(`/acts/${a.actor}/runs?waitForFinish=60`, { method: 'POST', body: JSON.stringify(a.input(handle, since)) })).data;
  for (let i = 0; i < 12 && (run.status === 'RUNNING' || run.status === 'READY'); i++) run = (await apify(`/actor-runs/${run.id}?waitForFinish=60`)).data;
  if (run.status !== 'SUCCEEDED') throw new Error(`Scrape ${run.status.toLowerCase()}`);
  const items = await apify(`/datasets/${run.defaultDatasetId}/items?clean=true&limit=${LIMIT * 2}`);
  return items.map(a.map).filter(v => v.id && Number.isFinite(+v.views) && v.date && +new Date(v.date) >= cutoff);
}

/* ---------- turning scrapes into daily numbers ---------- */
// videos: { id: { p: publishDay, v: viewsAtLastSync } }   gains: { day: viewsGained }
function ingest(acc, items, day = today()) {
  acc.videos ||= {}; acc.gains ||= {};
  let gained = 0;
  acc.gains[day] ||= 0;
  for (const it of items) {
    const views = Math.max(0, Math.round(+it.views));
    const p = iso(new Date(it.date));
    const prev = acc.videos[it.id];
    // Seen before: count the growth since the last check, on today.
    if (prev) { const d = Math.max(0, views - prev.v); acc.gains[day] += d; gained += d; }
    // New recent clip: everything it has so far counts on the day it was posted.
    else { acc.gains[p] = (acc.gains[p] || 0) + views; gained += views; }
    acc.videos[it.id] = { p, v: prev ? Math.max(prev.v, views) : views };
  }
  const old = iso(new Date(Date.now() - 120 * 864e5)); // keep the stored data small
  for (const [id, v] of Object.entries(acc.videos)) if (v.p < old) delete acc.videos[id];
  acc.baselined = true; acc.v2 = true; acc.lastSync = Date.now(); acc.error = null;
  return gained;
}
// One-time upgrade for accounts first checked under the old rules: keep only the last FIRST_DAYS days of views.
function migrate() {
  const cut = iso(new Date(Date.now() - FIRST_DAYS * 864e5));
  for (const u of Object.values(db.users)) for (const a of u.accounts || []) {
    if (a.v2) continue;
    a.gains = {};
    for (const v of Object.values(a.videos || {})) if (v.p >= cut) a.gains[v.p] = (a.gains[v.p] || 0) + v.v;
    a.v2 = true;
  }
  save();
}
const busy = new Set();
async function syncAccount(acc) {
  if (busy.has(acc.id) || !APIFY_TOKEN) return;
  busy.add(acc.id);
  try {
    const first = !acc.baselined;
    const items = await scrape(acc.platform, acc.handle, first);
    if (first && !items.length) throw new Error(`No clips posted in the last ${FIRST_DAYS} days — check the username`);
    ingest(acc, items);
  } catch (e) { acc.error = String(e.message || e).slice(0, 200); acc.lastTry = Date.now(); }
  finally { busy.delete(acc.id); save(); }
}
async function syncUser(u, force) {
  for (const acc of u.accounts) {
    const last = Math.max(acc.lastSync || 0, force ? 0 : acc.lastTry || 0);
    if (force || Date.now() - last > SYNC_HOURS * 36e5 - 6e5) await syncAccount(acc);
  }
}
async function tick() { for (const u of Object.values(db.users)) await syncUser(u, false).catch(() => {}); }

function stateFor(u) {
  const daily = {};
  const put = (d, id) => ((daily[d] ||= {})[id] ||= { views: 0, clips: 0 });
  for (const a of u.accounts) {
    for (const [d, v] of Object.entries(a.gains || {})) put(d, a.id).views = v;
    for (const v of Object.values(a.videos || {})) put(v.p, a.id).clips++;
  }
  return {
    rate: u.rate ?? 300, daily, syncHours: SYNC_HOURS, apify: !!APIFY_TOKEN,
    accounts: u.accounts.map(a => ({ id: a.id, platform: a.platform, handle: a.handle, lastSync: a.lastSync || null, error: a.error || null, syncing: busy.has(a.id), tracked: Object.keys(a.videos || {}).length })),
  };
}

/* ---------- http helpers ---------- */
const cookies = req => Object.fromEntries((req.headers.cookie || '').split(';').map(c => c.trim().split('=')).filter(p => p[0]).map(([k, ...v]) => [k, decodeURIComponent(v.join('='))]));
const setCookie = (name, val, maxAge) => `${name}=${encodeURIComponent(val)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}${secure ? '; Secure' : ''}`;
const json = (res, code, obj, headers = {}) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...headers }); res.end(JSON.stringify(obj)); };
const redirect = (res, to, headers = {}) => { res.writeHead(302, { Location: to, ...headers }); res.end(); };
function readBody(req) {
  return new Promise((ok, bad) => {
    if (!/^application\/json/.test(req.headers['content-type'] || '')) return bad(new Error('JSON required'));
    let s = ''; req.on('data', c => { s += c; if (s.length > 1e5) req.destroy(); });
    req.on('end', () => { try { ok(JSON.parse(s || '{}')); } catch { bad(new Error('Bad JSON')); } });
  });
}
function userOf(req) {
  const s = db.sessions[cookies(req).sid];
  if (!s || s.exp < Date.now()) return null;
  return db.users[s.uid] || null;
}
function login(res, uid, profile) {
  const u = (db.users[uid] ||= { rate: 300, accounts: [] });
  Object.assign(u, profile);
  const sid = crypto.randomBytes(32).toString('hex');
  db.sessions[sid] = { uid, exp: Date.now() + 30 * 864e5 };
  for (const [k, s] of Object.entries(db.sessions)) if (s.exp < Date.now()) delete db.sessions[k];
  save();
  redirect(res, '/', { 'Set-Cookie': [setCookie('sid', sid, 30 * 86400), setCookie('oauth', '', 0)] });
}

/* ---------- routes ---------- */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, BASE_URL);
  const p = url.pathname;
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return fs.createReadStream(path.join(ROOT, 'public', 'index.html')).pipe(res);
    }

    if (p === '/healthz') return json(res, 200, { ok: true });

    /* --- auth --- */
    if (p === '/auth/google') {
      if (!googleOn) return redirect(res, '/');
      const state = crypto.randomBytes(16).toString('hex');
      const q = new URLSearchParams({ client_id: GOOGLE_ID, redirect_uri: BASE_URL + '/auth/google/callback', response_type: 'code', scope: 'openid email profile', state, prompt: 'select_account' });
      return redirect(res, 'https://accounts.google.com/o/oauth2/v2/auth?' + q, { 'Set-Cookie': setCookie('oauth', state, 600) });
    }
    if (p === '/auth/google/callback') {
      const state = url.searchParams.get('state'), code = url.searchParams.get('code');
      if (!googleOn || !code || !state || state !== cookies(req).oauth) return redirect(res, '/?error=login');
      const r = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ code, client_id: GOOGLE_ID, client_secret: GOOGLE_SECRET, redirect_uri: BASE_URL + '/auth/google/callback', grant_type: 'authorization_code' }) });
      const tok = await r.json();
      if (!r.ok || !tok.id_token) return redirect(res, '/?error=login');
      const c = JSON.parse(Buffer.from(tok.id_token.split('.')[1], 'base64url').toString());
      if (c.aud !== GOOGLE_ID || !c.sub) return redirect(res, '/?error=login');
      if (ALLOWED.length && !(c.email_verified && ALLOWED.includes(String(c.email || '').toLowerCase()))) return redirect(res, '/?error=notallowed');
      return login(res, 'g:' + c.sub, { email: c.email, name: c.name || c.email, picture: c.picture || '' });
    }
    if (p === '/auth/local' && req.method === 'POST') {
      // Only while Google sign-in is not configured: one local profile on this computer.
      if (googleOn || hosted) return json(res, 403, { error: 'Use Google sign-in' });
      await readBody(req);
      return login(res, 'local', { email: '', name: 'Local profile', picture: '' });
    }
    if (p === '/auth/logout' && req.method === 'POST') {
      await readBody(req);
      delete db.sessions[cookies(req).sid]; save();
      return json(res, 200, { ok: true }, { 'Set-Cookie': setCookie('sid', '', 0) });
    }

    /* --- api --- */
    if (p.startsWith('/api/')) {
      const u = userOf(req);
      if (p === '/api/me') return json(res, 200, { user: u && { name: u.name, email: u.email, picture: u.picture }, google: googleOn, local: !googleOn && !hosted, apify: !!APIFY_TOKEN });
      if (!u) return json(res, 401, { error: 'Sign in first' });
      if (p === '/api/state' && req.method === 'GET') return json(res, 200, stateFor(u));
      if (p === '/api/accounts' && req.method === 'POST') {
        const b = await readBody(req), handle = clean(b.handle || '');
        if (!ACTORS[b.platform] || !/^[\w.\-]{1,60}$/.test(handle)) return json(res, 400, { error: 'Enter a valid username' });
        if (u.accounts.some(a => a.platform === b.platform && a.handle.toLowerCase() === handle.toLowerCase())) return json(res, 400, { error: 'Already added' });
        if (u.accounts.length >= 30) return json(res, 400, { error: 'Account limit reached' });
        const acc = { id: 'a' + crypto.randomBytes(6).toString('hex'), platform: b.platform, handle };
        u.accounts.push(acc); save(); syncAccount(acc);
        return json(res, 200, stateFor(u));
      }
      const del = p.match(/^\/api\/accounts\/(\w+)$/);
      if (del && req.method === 'DELETE') { u.accounts = u.accounts.filter(a => a.id !== del[1]); save(); return json(res, 200, stateFor(u)); }
      if (p === '/api/rate' && req.method === 'PUT') {
        const r = +(await readBody(req)).rate;
        if (!(r >= 0 && r < 1e6)) return json(res, 400, { error: 'Bad rate' });
        u.rate = r; save(); return json(res, 200, { ok: true });
      }
      if (p === '/api/sync' && req.method === 'POST') {
        await readBody(req);
        if (!APIFY_TOKEN) return json(res, 400, { error: 'Apify token is not set' });
        syncUser(u, true).catch(() => {});
        return json(res, 200, stateFor(u));
      }
    }
    json(res, 404, { error: 'Not found' });
  } catch (e) { json(res, 400, { error: String(e.message || e) }); }
});

if (require.main === module) loadDb().then(() => {
  migrate();
  console.log(`  Storage:        ${KV_URL ? 'Upstash (online)' : 'local file'}${hosted && !KV_URL ? ' — WARNING: data will be lost on restart, set UPSTASH_REDIS_REST_URL / _TOKEN' : ''}`);
  if (hosted && !ALLOWED.length) console.log('  WARNING: ALLOWED_EMAILS is not set — anyone with a Google account can sign in and use your Apify credits');
  server.listen(PORT, () => {
    console.log(`Space running at ${BASE_URL}`);
    console.log(`  Google sign-in: ${googleOn ? 'on' : 'OFF (local profile only) — set GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET in .env'}`);
    console.log(`  Apify sync:     ${APIFY_TOKEN ? `on, every ${SYNC_HOURS}h` : 'OFF — set APIFY_TOKEN in .env'}`);
  });
  setTimeout(tick, 5000);
  setInterval(tick, 15 * 60 * 1000);
}).catch(e => { console.error('Could not start:', e.message); process.exit(1); });
module.exports = { ingest, clean, ACTORS };
