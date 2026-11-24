use std::process::Stdio;

use serde_json::Value;
use tempfile::TempDir;
use tokio::process::Command;

#[tokio::test]
async fn missing_dsh_runtime_fails_before_claiming_acp_stdio() {
    let output = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
        .arg("--dsh-command")
        .arg("/definitely/missing/dsh")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("pinned DeepSeek Harness runtime"));
    assert!(stderr.contains(dsh_acp::dsh::PINNED_DSH_VERSION));
}

#[tokio::test]
async fn wrong_dsh_runtime_version_fails_before_initialize() {
    let temp = TempDir::new().unwrap();
    let script = temp.path().join("wrong-dsh.mjs");
    std::fs::write(
        &script,
        r#"
import readline from 'node:readline';
const rl = readline.createInterface({ input: process.stdin });
for await (const line of rl) {
  const request = JSON.parse(line);
  if (request.method !== 'dsh/hello') continue;
  process.stdout.write(JSON.stringify({
    jsonrpc: '2.0',
    id: request.id,
    result: { runtime: '@deepseek-ai/dsh', version: '0.2.1' }
  }) + '\n');
}
"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dsh-acp"))
        .arg("--dsh-js")
        .arg(&script)
        .arg("--node")
        .arg(std::env::var("DSH_ACP_TEST_NODE").unwrap_or_else(|_| "node".to_string()))
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(dsh_acp::dsh::PINNED_DSH_VERSION));
    assert!(stderr.contains("0.2.1"));
}

#[test]
fn runtime_version_is_pinned_in_npm_and_plugin_manifests() {
    let npm: Value = serde_json::from_str(include_str!("../npm/package.json")).unwrap();
    assert_eq!(
        npm["dependencies"]["@deepseek-ai/dsh"],
        dsh_acp::dsh::PINNED_DSH_VERSION
    );
    let plugin: Value = serde_json::from_str(include_str!("../dsh-plugin/package.json")).unwrap();
    assert_eq!(plugin["engines"]["dsh"], dsh_acp::dsh::PINNED_DSH_VERSION);
}
