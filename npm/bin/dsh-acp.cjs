#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');
const { createRequire } = require('node:module');
const path = require('node:path');

function targetFor(platform, arch, glibc) {
  const targets = {
    'darwin-x64': 'x86_64-apple-darwin',
    'darwin-arm64': 'aarch64-apple-darwin',
    'linux-x64': 'x86_64-unknown-linux-gnu',
    'linux-arm64': 'aarch64-unknown-linux-gnu',
    'win32-x64': 'x86_64-pc-windows-msvc',
    'win32-arm64': 'aarch64-pc-windows-msvc',
  };
  const target = targets[`${platform}-${arch}`];
  if (!target || (platform === 'linux' && !glibc)) {
    throw new Error(`Unsupported platform: ${platform}/${arch}${platform === 'linux' && !glibc ? ' (musl)' : ''}. ` +
      'dsh-acp supports macOS x64/arm64, Linux glibc x64/arm64, and Windows x64/arm64.');
  }
  return target;
}

function main() {
  let binary;
  try {
    const glibc = process.platform !== 'linux' || process.report.getReport().header.glibcVersionRuntime;
    const target = targetFor(process.platform, process.arch, glibc);
    binary = path.join(__dirname, '..', 'native', target, process.platform === 'win32' ? 'dsh-acp.exe' : 'dsh-acp');
  } catch (error) {
    console.error(`dsh-acp: ${error.message}`);
    process.exitCode = 1;
    return;
  }

  const env = { ...process.env };
  const args = process.argv.slice(2);
  const localOnly = args.includes('--version') || args.includes('--help') || args.includes('-h');
  if (!localOnly && !env.DSH_ACP_DSH_COMMAND && !env.DSH_ACP_DSH_JS) {
    try {
      const requireFromPackage = createRequire(__filename);
      const manifest = requireFromPackage.resolve('@deepseek-ai/dsh/package.json');
      const packageRoot = path.dirname(manifest);
      env.DSH_ACP_DSH_JS = path.join(packageRoot, 'lib', 'bin.js');
      env.DSH_ACP_NODE = process.execPath;
      env.DSH_ACP_RUNTIME_MODULES = path.resolve(packageRoot, '..', '..');
    } catch (error) {
      console.error(`dsh-acp: Could not locate the pinned @deepseek-ai/dsh runtime: ${error.message}`);
      process.exitCode = 1;
      return;
    }
  }

  // Keep ACP stdin/stdout byte-for-byte intact, including when launched by npx.
  const child = spawn(binary, args, { stdio: 'inherit', env });
  const handlers = new Map();
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
    const handler = () => child.kill(signal);
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  const cleanup = () => {
    for (const [signal, handler] of handlers) process.removeListener(signal, handler);
  };
  child.on('error', (error) => {
    cleanup();
    console.error(`dsh-acp: Could not start ${binary}: ${error.message}`);
    process.exitCode = 1;
  });
  child.on('exit', (code, signal) => {
    cleanup();
    if (signal && process.platform !== 'win32') process.kill(process.pid, signal);
    else process.exitCode = code ?? 1;
  });
}

if (require.main === module) main();
module.exports = { targetFor };
