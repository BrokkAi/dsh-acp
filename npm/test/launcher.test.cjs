'use strict';

const assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');
const { targetFor } = require('../bin/dsh-acp.cjs');

function fixture(t, withBinary = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-acp-npm-'));
  // Windows can briefly keep an exited child's files locked (EPERM/EBUSY);
  // rmSync retries those errors.
  t.after(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
  fs.mkdirSync(path.join(root, 'bin'));
  const launcher = path.join(root, 'bin/dsh-acp.cjs');
  fs.copyFileSync(path.join(__dirname, '../bin/dsh-acp.cjs'), launcher);
  const runtime = path.join(root, 'node_modules/@deepseek-ai/dsh');
  fs.mkdirSync(path.join(runtime, 'lib'), { recursive: true });
  fs.writeFileSync(path.join(runtime, 'package.json'), JSON.stringify({
    name: '@deepseek-ai/dsh',
    version: '0.2.2-rc.5',
    exports: { './package.json': './package.json' },
  }));
  fs.writeFileSync(path.join(runtime, 'lib/bin.js'), '');
  if (withBinary) {
    const native = path.join(root, 'native', targetFor(process.platform, process.arch, true));
    fs.mkdirSync(native, { recursive: true });
    // Node stands in for the native binary so we can observe arguments and IO.
    fs.copyFileSync(process.execPath, path.join(native, process.platform === 'win32' ? 'dsh-acp.exe' : 'dsh-acp'));
  }
  return launcher;
}

test('all supported platforms resolve, unsupported architectures and musl fail', () => {
  for (const [platform, arch, target] of [
    ['darwin', 'x64', 'x86_64-apple-darwin'],
    ['darwin', 'arm64', 'aarch64-apple-darwin'],
    ['linux', 'x64', 'x86_64-unknown-linux-gnu'],
    ['linux', 'arm64', 'aarch64-unknown-linux-gnu'],
    ['win32', 'x64', 'x86_64-pc-windows-msvc'],
    ['win32', 'arm64', 'aarch64-pc-windows-msvc'],
  ]) assert.equal(targetFor(platform, arch, true), target);
  for (const args of [['linux', 'x64', false], ['win32', 'ia32', true], ['linux', 'ia32', true], ['freebsd', 'x64', true]]) {
    assert.throws(() => targetFor(...args), /Unsupported platform/);
  }
});

test('preserves arguments, cwd, environment, stdin, stdout, stderr and exit code', (t) => {
  const launcher = fixture(t);
  const program = `
    process.stdin.on('data', data => process.stdout.write(data));
    process.stdin.on('end', () => {
      process.stdout.write(JSON.stringify({ args: process.argv.slice(1), cwd: process.cwd(), env: process.env.DSH_ACP_NPM_TEST }));
      process.stderr.write('child diagnostic');
      process.exitCode = 17;
    });
  `;
  const args = ['spaces and "quotes"', '$literal; not a shell'];
  const result = spawnSync(process.execPath, [launcher, '-e', program, '--', ...args], {
    input: 'ACP input\n', encoding: 'utf8', env: { ...process.env, DSH_ACP_NPM_TEST: 'inherited' }, timeout: 10000,
  });
  assert.equal(result.status, 17, result.stderr);
  assert.equal(result.stdout, 'ACP input\n' + JSON.stringify({ args, cwd: process.cwd(), env: 'inherited' }));
  assert.equal(result.stderr, 'child diagnostic');
});

test('resolves and passes the pinned DSH runtime and module root', (t) => {
  const launcher = fixture(t);
  const result = spawnSync(process.execPath, [launcher, '-e', `
    process.stdout.write(JSON.stringify({
      js: process.env.DSH_ACP_DSH_JS,
      node: process.env.DSH_ACP_NODE,
      modules: process.env.DSH_ACP_RUNTIME_MODULES
    }));
  `], { encoding: 'utf8', timeout: 10000 });
  assert.equal(result.status, 0, result.stderr);
  const resolved = JSON.parse(result.stdout);
  assert.match(resolved.js, /@deepseek-ai[/\\]dsh[/\\]lib[/\\]bin\.js$/);
  assert.match(resolved.modules, /node_modules$/);
  assert.equal(path.resolve(resolved.node), path.resolve(process.execPath));
});

test('missing binary fails without contaminating ACP stdout', (t) => {
  const result = spawnSync(process.execPath, [fixture(t, false)], { encoding: 'utf8', timeout: 10000 });
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /Could not start.*ENOENT/);
});

test('forwards termination to the child and preserves its signal exit', { skip: process.platform === 'win32', timeout: 10000 }, async (t) => {
  const child = spawn(process.execPath, [fixture(t), '-e', `
    process.on('SIGTERM', () => {
      process.stderr.write('received SIGTERM');
      process.removeAllListeners('SIGTERM');
      process.kill(process.pid, 'SIGTERM');
    });
    process.stdout.write('ready');
    setInterval(() => {}, 1000);
  `]);
  t.after(() => child.kill('SIGKILL'));
  let stderr = '';
  child.stderr.on('data', data => { stderr += data; });
  const closed = once(child, 'close');
  await once(child.stdout, 'data');
  child.kill('SIGTERM');
  const [code, signal] = await closed;
  assert.equal(stderr, 'received SIGTERM');
  assert.equal(code, null);
  assert.equal(signal, 'SIGTERM');
});
