// Smoke-test the launcher against the native binary on each release runner.
'use strict';
const assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const readline = require('node:readline');
const { targetFor } = require('../npm/bin/dsh-acp.cjs');

const version = fs.readFileSync(path.join(__dirname, '../Cargo.toml'), 'utf8').match(/^version = "([^"]+)"/m)[1];

// An ACP handshake needs no API key, so it proves the launcher passes stdio
// through unchanged without any provider configuration. Like a real client,
// keep stdin open until the response arrives; the server exits on EOF.
async function initialize(launcher, dataDir) {
  const env = { ...process.env };
  delete env.DEEPSEEK_API_KEY;
  const child = spawn(process.execPath, [launcher, '--data-dir', dataDir], { env, stdio: ['pipe', 'pipe', 'inherit'] });
  const exited = once(child, 'exit');
  const timer = setTimeout(() => child.kill(), 30000);
  try {
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: 1 } }) + '\n');
    let response;
    for await (const line of readline.createInterface({ input: child.stdout })) {
      const frame = JSON.parse(line);
      if (frame.id === 1) {
        response = frame;
        break;
      }
    }
    child.stdin.end();
    const [code, signal] = await exited;
    assert.equal(response?.result?.agentInfo?.name, 'dsh-acp', JSON.stringify(response));
    assert.equal(response.result.agentInfo.version, version);
    assert.deepEqual([code, signal], [0, null], 'server should exit cleanly on stdin EOF');
  } finally {
    clearTimeout(timer);
  }
}

async function main(root) {
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

  await initialize(launcher, path.join(root, 'data'));
  process.stdout.write('npm launcher: ACP initialize OK\n');
}

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-acp-npm-smoke-'));
main(root)
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
