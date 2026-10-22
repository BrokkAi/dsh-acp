// Smoke-test the launcher against the native binary on each release runner.
'use strict';
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { targetFor } = require('../npm/bin/dsh-acp.cjs');

const version = fs.readFileSync(path.join(__dirname, '../Cargo.toml'), 'utf8').match(/^version = "([^"]+)"/m)[1];
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-acp-npm-smoke-'));
try {
  const target = targetFor(process.platform, process.arch, true);
  const binary = process.platform === 'win32' ? 'dsh-acp.exe' : 'dsh-acp';
  const native = path.join(root, 'native', target);
  fs.mkdirSync(native, { recursive: true });
  fs.mkdirSync(path.join(root, 'bin'));
  fs.copyFileSync(path.join(__dirname, '../npm/bin/dsh-acp.cjs'), path.join(root, 'bin/dsh-acp.cjs'));
  fs.copyFileSync(path.resolve(process.argv[2] || path.join('target', target, 'release', binary)), path.join(native, binary));
  const launcher = path.join(root, 'bin/dsh-acp.cjs');

  const result = spawnSync(process.execPath, [launcher, '--version'], { encoding: 'utf8', timeout: 30000 });
  assert.equal(result.status, 0, result.stderr || String(result.error));
  assert.equal(result.stdout.trim(), `dsh-acp ${version}`);
  process.stdout.write(result.stdout);

  // An ACP handshake needs no API key, so it proves the launcher passes
  // stdio through unchanged without any provider configuration.
  const env = { ...process.env };
  delete env.DEEPSEEK_API_KEY;
  const handshake = spawnSync(process.execPath, [launcher, '--data-dir', path.join(root, 'data')], {
    encoding: 'utf8', timeout: 30000, env,
    input: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: 1 } }) + '\n',
  });
  assert.equal(handshake.status, 0, handshake.stderr || String(handshake.error));
  const frames = handshake.stdout.trim().split('\n').map(line => JSON.parse(line));
  const response = frames.find(frame => frame.id === 1);
  assert.equal(response?.result?.agentInfo?.name, 'dsh-acp', JSON.stringify(frames));
  assert.equal(response.result.agentInfo.version, version);
  process.stdout.write('npm launcher: ACP initialize OK\n');
} finally {
  fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}
