// Minimal HTTP wrapper around `claude -p`. Usage: node server.mjs
// POST /run  {"prompt": "...", "cwd": "C:\\work\\workspace", "sessionId": "<uuid to resume>"}
import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const PORT = +(process.env.PORT || 8765);
const BIN = process.env.CLAUDE_BIN || 'claude';
const TOKEN = process.env.CLAUDE_HTTP_TOKEN || '';
const HOST = process.env.HOST || '0.0.0.0'; // set HOST=127.0.0.1 to accept local connections only
if (!TOKEN && !['127.0.0.1', 'localhost', '::1'].includes(HOST)) {
  console.error('Refusing to listen on ' + HOST + ' without CLAUDE_HTTP_TOKEN set.');
  process.exit(1);
}
const TIMEOUT_MS = +(process.env.CLAUDE_TIMEOUT_MS || 600000);
// Fixed at startup, never from the request, e.g. --allowedTools "Read,Grep" or --permission-mode acceptEdits
const EXTRA_ARGS = (process.env.CLAUDE_ARGS || '').match(/"[^"]*"|\S+/g)?.map(a => a.replace(/^"|"$/g, '')) || [];
const DEBUG = !!process.env.DEBUG; // DEBUG=1 logs every request, spawn, child output and response
const LOG_FILE = process.env.LOG_FILE; // LOG_FILE=claude-http.log also appends those log lines to a file
const logStream = LOG_FILE && fs.createWriteStream(LOG_FILE, { flags: 'a' });
const log = (...a) => {
  if (!DEBUG && !logStream) return;
  const line = [new Date().toISOString(), ...a].join(' ');
  if (DEBUG) console.log(line);
  logStream?.write(line + '\n');
};
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function runClaude({ prompt, cwd, sessionId }) {
  return new Promise((resolve) => {
    const args = ['-p', '--output-format', 'json', ...EXTRA_ARGS];
    if (sessionId) args.push('--resume', sessionId);
    // Prompt goes via stdin so it never touches a shell command line (.cmd shims on Windows need shell:true).
    log('spawn', BIN, args.join(' '), 'cwd=' + (cwd || process.cwd()));
    const child = spawn(BIN, args, { cwd: cwd || process.cwd(), shell: /\.(cmd|bat)$/i.test(BIN) || (process.platform === 'win32' && BIN === 'claude') });
    let out = '', err = '';
    const timer = setTimeout(() => { log('timeout after', TIMEOUT_MS, 'ms, killing pid', child.pid); child.kill(); }, TIMEOUT_MS);
    child.stdout.on('data', d => { out += d; log('stdout:', String(d)); });
    child.stderr.on('data', d => { err += d; log('stderr:', String(d)); });
    child.on('error', e => { clearTimeout(timer); resolve({ code: -1, out, err: String(e) }); });
    child.on('close', code => { clearTimeout(timer); resolve({ code, out, err }); });
    child.stdin.end(prompt);
  });
}

const send = (res, status, body) => {
  log('response', status, typeof body === 'string' ? body : JSON.stringify(body));
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};

http.createServer((req, res) => {
  const t0 = Date.now();
  res.on('finish', () => log('done in', Date.now() - t0, 'ms'));
  log('request', req.method, req.url, 'from', req.socket.remoteAddress);
  if (TOKEN && req.headers.authorization !== `Bearer ${TOKEN}`) return send(res, 401, { error: 'unauthorized' });
  if (req.method !== 'POST' || req.url !== '/run') return send(res, 404, { error: 'POST /run only' });
  let raw = '';
  req.on('data', d => { raw += d; if (raw.length > 1e6) req.destroy(); });
  req.on('end', async () => {
    log('body', raw);
    let body;
    try { body = JSON.parse(raw); } catch { return send(res, 400, { error: 'invalid JSON' }); }
    if (typeof body.prompt !== 'string' || !body.prompt.trim()) return send(res, 400, { error: 'prompt required' });
    if (body.sessionId && !UUID.test(body.sessionId)) return send(res, 400, { error: 'sessionId must be a UUID' });
    if (body.cwd && typeof body.cwd !== 'string') return send(res, 400, { error: 'cwd must be a string' });
    const r = await runClaude(body);
    if (r.code !== 0) return send(res, 500, { error: 'claude failed', code: r.code, stderr: r.err, stdout: r.out });
    send(res, 200, r.out); // claude's own JSON: result, session_id, total_cost_usd, ...
  });
}).listen(PORT, HOST, () => { console.log(`claude-http on http://${HOST}:${PORT}/run -> ${BIN}`); log('listening on', HOST + ':' + PORT); });
