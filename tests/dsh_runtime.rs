use std::{path::PathBuf, time::Duration};

use dsh_acp::dsh::{DshRuntime, PINNED_DSH_VERSION, RuntimeConfig};
use serde_json::json;

#[tokio::test]
#[ignore = "requires the pinned official DeepSeek Harness runtime"]
async fn opens_lists_and_closes_a_real_dsh_session() {
    let dsh_js = std::env::var_os("DSH_ACP_REAL_DSH_JS")
        .map(PathBuf::from)
        .expect("DSH_ACP_REAL_DSH_JS must name @deepseek-ai/dsh/lib/bin.js");
    let data = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let runtime = DshRuntime::start(RuntimeConfig {
        command: std::env::var_os("DSH_ACP_TEST_NODE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("node")),
        prefix_args: vec![dsh_js.display().to_string()],
        profile: "sdk".to_string(),
        home: Some(home.path().to_path_buf()),
        runtime_modules: None,
        api_key: Some("runtime-test".to_string()),
        base_url: None,
        data_dir: data.path().to_path_buf(),
        request_timeout: Duration::from_secs(60),
    })
    .await
    .unwrap();

    let hello = runtime.request("dsh/hello", json!({})).await.unwrap();
    assert_eq!(hello["version"], PINNED_DSH_VERSION);
    runtime
        .request(
            "dsh/initialize",
            json!({
                "provider": "deepseek-official",
                "model": "deepseek-v4-flash",
                "reasoningEffort": "high"
            }),
        )
        .await
        .unwrap();
    let opened = runtime
        .request(
            "dsh/session/open",
            json!({
                "sessionId": "runtime-test",
                "cwd": workspace.path(),
                "provider": "deepseek-official",
                "model": "deepseek-v4-flash"
            }),
        )
        .await
        .unwrap();
    assert_eq!(opened["sessionId"], "runtime-test");
    assert_eq!(opened["cwd"], workspace.path().to_string_lossy().as_ref());
    runtime
        .request(
            "dsh/session/fork",
            json!({
                "sourceSessionId": "runtime-test",
                "childSessionId": "runtime-child",
                "cwd": workspace.path(),
                "provider": "deepseek-official",
                "model": "deepseek-v4-flash"
            }),
        )
        .await
        .unwrap();
    let listed = runtime
        .request("dsh/session/list", json!({}))
        .await
        .unwrap();
    assert!(
        listed["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["sessionId"] == "runtime-test")
    );
    assert!(
        listed["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["sessionId"] == "runtime-child")
    );
    runtime
        .request("dsh/session/close", json!({"sessionId": "runtime-child"}))
        .await
        .unwrap();
    runtime
        .request("dsh/session/delete", json!({"sessionId": "runtime-child"}))
        .await
        .unwrap();
    let after_delete = runtime
        .request("dsh/session/list", json!({}))
        .await
        .unwrap();
    assert!(
        !after_delete["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["sessionId"] == "runtime-child")
    );
    runtime
        .request("dsh/session/close", json!({"sessionId": "runtime-test"}))
        .await
        .unwrap();
    runtime.close().await;
}
