const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require('playwright');

const root = path.resolve(__dirname, '..');
const example = path.join(root, 'examples/worker-ledger');
const web = path.join(example, 'web');

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${command} failed`);
}

run('cargo', ['build', '--manifest-path', path.join(example, 'Cargo.toml'), '--lib', '--target', 'wasm32-unknown-unknown']);
run(process.env.WASM_BINDGEN ?? 'wasm-bindgen', [
  '--target', 'web', '--out-dir', path.join(web, 'pkg'),
  path.join(example, 'target/wasm32-unknown-unknown/debug/worker_ledger.wasm'),
]);

async function main() {
  let gate;
  function waitForQuery() {
    assert.equal(gate, undefined);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('SQLite did not reach the progress gate')), 10000);
      gate = { timer, arrive(response) {
        clearTimeout(timer);
        gate.response = response;
        resolve(() => {
          response.end();
          gate = undefined;
        });
      } };
    });
  }
  const server = http.createServer((request, response) => {
    const url = new URL(request.url, 'http://localhost');
    if (url.pathname === '/query-gate') {
      if (gate) gate.arrive(response);
      else response.writeHead(500).end('no query expected');
      return;
    }
    const filename = path.resolve(web, `.${decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname)}`);
    if (!filename.startsWith(`${web}${path.sep}`) || !fs.existsSync(filename)) {
      response.writeHead(404).end();
      return;
    }
    const types = { '.js': 'text/javascript', '.wasm': 'application/wasm', '.html': 'text/html' };
    response.setHeader('Content-Type', types[path.extname(filename)] ?? 'application/octet-stream');
    fs.createReadStream(filename).pipe(response);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, executablePath: process.env.CHROMIUM });
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(origin);
    await page.evaluate(async () => {
      const caller = new Worker('/caller-worker.js', { type: 'module' });
      let nextId = 0;
      let failure;
      const pending = new Map();
      const fail = error => {
        failure = error;
        for (const task of pending.values()) {
          clearTimeout(task.timer);
          task.reject(error);
        }
        pending.clear();
        caller.terminate();
      };
      const wait = (id, label) => new Promise((resolve, reject) => {
        const timer = setTimeout(() => fail(new Error(`${label} timed out`)), 30000);
        pending.set(id, { resolve, reject, timer });
      });
      caller.onerror = event => fail(new Error(event.message || 'caller worker failed'));
      caller.onmessageerror = () => fail(new Error('caller response could not be decoded'));
      caller.onmessage = ({ data }) => {
        const id = data.ready ? 0 : data.id;
        const task = pending.get(id);
        if (!task) { fail(new Error(`Unknown response: ${id}`)); return; }
        clearTimeout(task.timer);
        pending.delete(id);
        if (data.error) task.reject(new Error(data.error));
        else task.resolve(data.result);
      };
      await wait(0, 'caller startup');
      window.call = (method, argument) => {
        if (failure) return Promise.reject(failure);
        const id = ++nextId;
        const result = wait(id, method);
        try { caller.postMessage({ id, method, argument }); }
        catch (error) { fail(error); }
        return result;
      };
    });
    const call = (method, argument) => page.evaluate(([method, argument]) => window.call(method, argument), [method, argument]);
    const url = (name, extra = '') => `${origin}/database-worker.js?name=${name}${extra}`;

    await call('open', url('contract.sqlite3'));
    await call('exercise');
    await call('cancel', '5');
    assert.equal(await call('balance'), '65');
    console.log('PASS shared transaction, refusal, rollback, errors, precision, correlation and caller cancellation');

    let queryFinished = false;
    const started = waitForQuery();
    const longQuery = call('long_query').then(result => { queryFinished = true; return result; });
    longQuery.catch(() => {});
    const release = await started;
    assert.equal(await call('ping'), 'caller-responsive');
    assert.equal(queryFinished, false, 'caller must respond while SQLite is executing in its own worker');
    release();
    assert.equal(await longQuery, '50005000');
    console.log('PASS caller worker progresses while database worker executes SQLite');

    const draining = await page.evaluate(async () => {
      const debit = window.call('debit', '5');
      const close = window.call('close');
      return Promise.all([debit, close]);
    });
    assert.equal(draining[0], '60');
    await call('close');
    await assert.rejects(call('balance'), /not dispatched|closed/i);
    await call('open', url('contract.sqlite3'));
    assert.equal(await call('balance'), '60');
    await call('close');
    console.log('PASS close drains accepted calls, rejects new calls, and OPFS survives worker replacement');

    await assert.rejects(call('open', url('failed.sqlite3', '&fail=1')), /requested initialization failure/);
    console.log('PASS startup failure');

    await call('open', url('cleanup.sqlite3', '&fail_cleanup=1'));
    await assert.rejects(call('close'), /requested cleanup failure/);
    await assert.rejects(call('close'), /requested cleanup failure/);
    await call('open', url('cleanup.sqlite3'));
    await call('close');
    console.log('PASS application cleanup completes before close acknowledgment');

    await call('open', url('lost-reply.sqlite3', '&lose_reply=1'));
    await assert.rejects(call('debit', '10'), /unknown/i);
    await call('open', url('lost-reply.sqlite3'));
    assert.equal(await call('balance'), '90');
    await call('close');
    console.log('PASS commit followed by lost reply reports unknown outcome without replay');

    await call('open', url('terminated.sqlite3'));
    const stopping = waitForQuery();
    const terminating = call('long_query').then(() => 'unexpected success', error => String(error));
    const releaseTerminated = await stopping;
    await call('terminate');
    assert.match(await terminating, /unknown/i);
    releaseTerminated();
    console.log('PASS explicit termination settles pending calls');
    assert.deepEqual(errors, []);
  } finally {
    if (gate) {
      clearTimeout(gate.timer);
      gate.response?.end();
    }
    await browser?.close();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
