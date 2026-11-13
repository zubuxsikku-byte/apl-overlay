// APL Overlay relay: controller -> server -> overlay (OBS / Prism)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const KEY = process.env.CONTROL_KEY || crypto.randomBytes(4).toString('hex'); // controller password
const DIR = path.join(__dirname, 'public');
const DATA = path.join(__dirname, 'data');
fs.mkdirSync(DATA, { recursive: true });

const PAGES = { '/controller': 'controller.html', '/overlay': 'overlay.html' };
const server = http.createServer((req, res) => {
  const p = new URL(req.url, 'http://x').pathname;
  if (p === '/') { res.writeHead(302, { Location: '/controller' }); return res.end(); }
  if (p === '/health') return res.end('ok');
  const f = PAGES[p];
  if (!f) { res.writeHead(404); return res.end('Not found'); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  fs.createReadStream(path.join(DIR, f)).on('error', () => res.end('missing file')).pipe(res);
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 * 1024 });
server.on('upgrade', (req, sock, head) => {
  if (new URL(req.url, 'http://x').pathname !== '/ws') return sock.destroy();
  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req));
});

const rooms = new Map();
function room(n) {
  if (!rooms.has(n)) {
    let st = {};
    try { st = JSON.parse(fs.readFileSync(path.join(DATA, n + '.json'), 'utf8')); } catch (e) {}
    rooms.set(n, { st, ov: new Set(), ct: new Set(), t: null });
  }
  return rooms.get(n);
}
function save(n, r) {
  clearTimeout(r.t);
  r.t = setTimeout(() => fs.writeFile(path.join(DATA, n + '.json'), JSON.stringify(r.st), () => {}), 1500);
}
const send = (ws, o) => { if (ws.readyState === 1) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };
function status(r) {
  const s = JSON.stringify({ t: 'status', overlays: r.ov.size, controllers: r.ct.size });
  r.ct.forEach(c => send(c, s));
}

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const name = (q.get('room') || 'main').replace(/[^\w-]/g, '').slice(0, 40) || 'main';
  const role = q.get('role') === 'controller' ? 'controller' : 'overlay';
  const r = room(name);
  ws.isAlive = true; ws.on('pong', () => (ws.isAlive = true));

  if (role === 'controller') {
    if (q.get('key') !== KEY) { send(ws, { t: 'denied' }); return ws.close(); }
    r.ct.add(ws);
    send(ws, { t: 'hello', cfg: r.st.cfg, m: r.st.m });
  } else {
    r.ov.add(ws);
    if (r.st.cfg) send(ws, { t: 'cfg', cfg: r.st.cfg });
    if (r.st.m) send(ws, { t: 'state', m: r.st.m, ui: r.st.ui });
    if (r.st.look) send(ws, r.st.look);
  }
  status(r);

  ws.on('message', buf => {
    if (role !== 'controller') return;
    const raw = buf.toString(); let d;
    try { d = JSON.parse(raw); } catch (e) { return; }
    if (d.t === 'cfg') { r.st.cfg = d.cfg; save(name, r); }
    else if (d.t === 'state') { r.st.m = d.m; r.st.ui = d.ui; save(name, r); }
    else if (d.t === 'look') { r.st.look = d; save(name, r); }
    else if (d.t !== 'call') return;
    r.ov.forEach(o => send(o, raw));
  });
  ws.on('close', () => { r.ov.delete(ws); r.ct.delete(ws); status(r); });
});

setInterval(() => wss.clients.forEach(w => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 25000);

server.listen(PORT, () => {
  console.log('APL server running on port ' + PORT);
  console.log('Controller key: ' + KEY + (process.env.CONTROL_KEY ? '' : '  (random - set CONTROL_KEY env to fix it)'));
  console.log('Controller: http://localhost:' + PORT + '/controller?room=match1&key=' + KEY);
  console.log('Overlay   : http://localhost:' + PORT + '/overlay?room=match1');
});
