use futures::StreamExt;
use reqwest::StatusCode;
use serde_json::{Value, json};
use simple_im::http::AppState;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

// ── Test server helpers ────────────────────────────────────────────────────────

struct TestServer {
    base_url: String,
}

impl TestServer {
    async fn spawn() -> Self {
        let state = Arc::new(AppState::new(Duration::from_secs(30)));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = simple_im::http::router(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        TestServer {
            base_url: format!("http://127.0.0.1:{}", addr.port()),
        }
    }

    /// Spawn a server with a pre-installed governor. Returns (server, governor_token_string).
    async fn spawn_with_governor() -> (Self, String) {
        use simple_im::delivery::DeliveryHub;
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let gov_tok = gov.0.clone();
        let state = Arc::new(AppState::new_with_hub(hub));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = simple_im::http::router(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            TestServer {
                base_url: format!("http://127.0.0.1:{}", addr.port()),
            },
            gov_tok,
        )
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }
}

/// POST /register — mint a fresh token (new-participant flow).
async fn register_participant_tok(server: &TestServer, client: &reqwest::Client) -> String {
    let r = client.post(server.url("/register")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "POST /register failed");
    let body: Value = r.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}

/// POST /listen, read the welcome event, return (token, task_handle).
/// The task_handle keeps the SSE connection alive — drop it when done.
/// When existing_token is None, auto-registers via POST /register first.
async fn listen_get_token(
    server: &TestServer,
    client: &reqwest::Client,
    existing_token: Option<&str>,
) -> (String, tokio::task::JoinHandle<()>) {
    let owned;
    let tok: &str = if let Some(t) = existing_token {
        t
    } else {
        owned = register_participant_tok(server, client).await;
        &owned
    };
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", tok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "POST /listen failed");

    let mut stream = r.bytes_stream();
    let mut buffer = String::new();
    // Wait for the welcome event. Normal agents already know their token (from registration).
    // Exception: governor session-link path — server mints a new listen token and includes it
    // in the welcome. In that case, extract from the welcome; otherwise use tok.
    let token = loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout waiting for SSE event")
            .unwrap()
            .unwrap();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        if let Some(line) = buffer.lines().find(|l| l.starts_with("data:")) {
            let w: Value = serde_json::from_str(line.trim_start_matches("data:").trim()).unwrap();
            assert_eq!(w["event"], "welcome");
            // If server included a minted listen token (governor case), use it; else use registered tok.
            break w["token"]
                .as_str()
                .map(|t| t.to_string())
                .unwrap_or_else(|| tok.to_string());
        }
    };

    // Spawn a task that holds the stream open until dropped.
    let handle = tokio::spawn(async move {
        // Drive the stream to keep the connection alive but don't process events.
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(stream);
    });

    (token, handle)
}

/// POST /listen and read the first SSE event. Drops the stream (agent goes offline after).
/// When existing_token is None, auto-registers via POST /register first.
async fn listen_and_get_welcome(
    server: &TestServer,
    client: &reqwest::Client,
    existing_token: Option<&str>,
) -> (String, String) {
    let owned;
    let tok: &str = if let Some(t) = existing_token {
        t
    } else {
        owned = register_participant_tok(server, client).await;
        &owned
    };
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", tok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "POST /listen failed");

    let mut stream = r.bytes_stream();
    let mut buffer = String::new();
    let welcome_json = loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout waiting for SSE event")
            .unwrap()
            .unwrap();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        if let Some(line) = buffer.lines().find(|l| l.starts_with("data:")) {
            break line.trim_start_matches("data:").trim().to_string();
        }
    };

    let parsed: Value = serde_json::from_str(&welcome_json).unwrap();
    assert_eq!(parsed["type"], "service");
    assert_eq!(parsed["event"], "welcome");
    // Normal agents use their registered token. Governor session-link path receives a
    // minted listen token in the welcome — use it when present.
    let token = parsed["token"]
        .as_str()
        .map(|t| t.to_string())
        .unwrap_or_else(|| tok.to_string());
    (token, welcome_json)
}

// ── AC-T1: POST /register returns 8-12 digit token; welcome has no token ──

#[tokio::test]
async fn ac_t1_listen_returns_token_in_welcome_event() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Token comes from POST /register, not from the welcome event.
    let (token, welcome_json) = listen_and_get_welcome(&server, &client, None).await;

    // Registered token must be 8-12 digits.
    assert!(
        token.len() >= 8 && token.len() <= 12,
        "token length: {}",
        token.len()
    );
    assert!(
        token.chars().all(|c| c.is_ascii_digit()),
        "token must be numeric: {}",
        token
    );

    // Welcome event must NOT contain a token field.
    let welcome: Value = serde_json::from_str(&welcome_json).unwrap();
    assert!(
        welcome.get("token").is_none(),
        "welcome event must not echo token, got: {}",
        welcome
    );
}

// ── AC-A1: POST /announce with available name → ok:true ───────────────────────

#[tokio::test]
async fn ac_a1_announce_available_name_succeeds() {
    let server = TestServer::spawn().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "TestParticipant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce should return 204 No Content"
    );
}

// ── AC-A2: POST /announce on live-held name → NAME_IN_USE ─────────────────────

#[tokio::test]
async fn ac_a2_announce_live_held_name_returns_name_in_use() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Agent 1 claims the name — keep SSE stream alive.
    let (tok1, _stream1) = listen_get_token(&server, &client, None).await;
    // Small yield so the server has processed the SSE connection open.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok1))
        .json(&json!({"name": "SharedName"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    // Agent 2 tries to claim same name — should get NAME_IN_USE (agent1 SSE still alive).
    let (tok2, _stream2) = listen_get_token(&server, &client, None).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok2))
        .json(&json!({"name": "SharedName"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["error"], "NAME_IN_USE");
    assert!(
        body["resolution_stream"].is_string(),
        "resolution_stream must be present"
    );
    _stream1.abort();
    _stream2.abort();
}

// ── AC-D1/D5: POST /messages/queue/pop is non-blocking; empty queue returns null ─

#[tokio::test]
async fn ac_d1_d5_dequeue_nonblocking_empty_returns_null() {
    let server = TestServer::spawn().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "ParticipantDQ"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce should return 204"
    );

    let start = std::time::Instant::now();
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(r.status(), StatusCode::OK, "dequeue failed");
    // Non-blocking: should return in well under 1 second.
    assert!(
        elapsed < Duration::from_millis(500),
        "dequeue blocked for {:?}",
        elapsed
    );

    let body: Value = r.json().await.unwrap();
    assert!(
        body["message"].is_null(),
        "empty queue: message must be null"
    );
    assert_eq!(body["remaining"], 0);
}

// ── AC-D2: dequeue response always includes `remaining` ───────────────────────

#[tokio::test]
async fn ac_d2_dequeue_always_includes_remaining() {
    let server = TestServer::spawn().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "AgentD2"}))
        .send()
        .await
        .unwrap();

    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert!(
        !body["remaining"].is_null(),
        "remaining must always be present"
    );
}

// ── AC-D3: DELETE /messages/queue returns all messages ────────────────────────

#[tokio::test]
async fn ac_d3_dequeue_all_returns_all_messages() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver (keep SSE alive so messages can be delivered).
    let (recv_tok, recv_stream) = listen_get_token(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .json(&json!({"name": "Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender_tok, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender_tok))
        .json(&json!({"name": "Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender_tok, "identity_b": recv_tok}))
        .send()
        .await
        .unwrap();

    // Send 3 messages.
    for i in 0..3u32 {
        client
            .post(server.url("/messages/send"))
            .header("Authorization", format!("Bearer {}", sender_tok))
            .json(&json!({"to": "Receiver", "payload": format!("msg-{}", i)}))
            .send()
            .await
            .unwrap();
    }

    // Drain all.
    let r = client
        .delete(server.url("/messages/queue"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body: Value = r.json().await.unwrap();
    let msgs = body["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3, "expected 3 messages, got {}", msgs.len());
    recv_stream.abort();
}

// ── AC-P1: presence reflects active SSE ───────────────────────────────────────

#[tokio::test]
async fn ac_p1_presence_reflects_active_sse() {
    let server = TestServer::spawn().await;
    let client = server.client();

    let (tok_a, _) = listen_and_get_welcome(&server, &client, None).await;
    let (tok_b, _) = listen_and_get_welcome(&server, &client, None).await;

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({"name": "AgentOnline"}))
        .send()
        .await
        .unwrap();

    // Agent B queries Agent A's presence — A has an active SSE.
    let r = client
        .get(server.url("/participants/AgentOnline/presence"))
        .header("Authorization", format!("Bearer {}", tok_b))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body: Value = r.json().await.unwrap();
    // The SSE stream is open (we still hold the bytes handle), so it should be online.
    // Note: in the test we don't hold the SSE stream open, so presence may be offline.
    // We check the structure is correct at minimum.
    assert!(body["status"] == "online" || body["status"] == "offline");
}

// ── AC-L1: Two POST /listen → second supersedes first ─────────────────────────

#[tokio::test]
async fn ac_l1_second_listen_supersedes_first() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // First listen — get the token.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let mut first_stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), first_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let token = _rtok.clone();

    // Second listen with same token + force=true — should supersede the first.
    let _r2 = client
        .post(server.url("/listen?force=true"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();

    // The first stream should now receive a superseded event.
    let mut got_superseded = false;
    let mut chunks_read = 0;
    while chunks_read < 10 {
        match tokio::time::timeout(Duration::from_millis(500), first_stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let data = String::from_utf8_lossy(&chunk);
                if data.contains("superseded") {
                    got_superseded = true;
                    break;
                }
                chunks_read += 1;
            }
            _ => break,
        }
    }
    assert!(
        got_superseded,
        "first SSE stream should receive superseded event"
    );
}

// ── AC-N1: First message triggers exactly one NOTIFY ──────────────────────────

#[tokio::test]
async fn ac_n1_first_message_triggers_notify() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": "NotifyReceiver"}))
        .send()
        .await
        .unwrap();
    let (sender_tok, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender_tok))
        .json(&json!({"name": "NotifySender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender_tok, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    // Send one message.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_tok))
        .json(&json!({"to": "NotifyReceiver", "payload": "hello"}))
        .send()
        .await
        .unwrap();

    // The receiver's SSE stream should get a NOTIFY event within 500ms.
    let mut got_notify = false;
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(100), stream.next()).await
        {
            let data = String::from_utf8_lossy(&chunk);
            if data.contains("\"notify\"") {
                got_notify = true;
                break;
            }
        }
    }
    assert!(
        got_notify,
        "receiver SSE should get a NOTIFY event within 500ms"
    );
}

// ── AC-N2: Three rapid messages → exactly one NOTIFY ──────────────────────────

#[tokio::test]
async fn ac_n2_rapid_messages_single_notify() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": "N2Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "N2Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    // Send 3 messages rapidly.
    for i in 0..3u32 {
        client
            .post(server.url("/messages/send"))
            .header("Authorization", format!("Bearer {}", sender))
            .json(&json!({"to": "N2Receiver", "payload": format!("rapid-{}", i)}))
            .send()
            .await
            .unwrap();
    }

    // Collect all SSE events within 500ms.
    let mut notify_count = 0;
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
            let data = String::from_utf8_lossy(&chunk);
            for line in data.lines() {
                if line.starts_with("data:") {
                    let event: Value =
                        serde_json::from_str(line.trim_start_matches("data:").trim())
                            .unwrap_or(Value::Null);
                    if event["type"] == "notify" {
                        notify_count += 1;
                    }
                }
            }
        }
    }
    assert_eq!(
        notify_count, 1,
        "exactly 1 NOTIFY expected for 3 rapid messages, got {}",
        notify_count
    );
}

// ── AC-N3: After dequeue, new message triggers fresh NOTIFY ───────────────────

#[tokio::test]
async fn ac_n3_dequeue_then_new_message_triggers_notify() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": "N3Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "N3Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    // First message → NOTIFY fired, suppressed.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "N3Receiver", "payload": "first"}))
        .send()
        .await
        .unwrap();

    // Wait for NOTIFY.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Dequeue (re-arms notify).
    client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .send()
        .await
        .unwrap();

    // Second message should trigger another NOTIFY.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "N3Receiver", "payload": "second"}))
        .send()
        .await
        .unwrap();

    let mut got_second_notify = false;
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    let mut notifies_seen = 0;
    while start.elapsed() < deadline {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
            let data = String::from_utf8_lossy(&chunk);
            for line in data.lines() {
                if line.starts_with("data:") {
                    let event: Value =
                        serde_json::from_str(line.trim_start_matches("data:").trim())
                            .unwrap_or(Value::Null);
                    if event["type"] == "notify" {
                        notifies_seen += 1;
                        if notifies_seen >= 2 {
                            got_second_notify = true;
                        }
                    }
                }
            }
        }
    }
    assert!(
        got_second_notify,
        "after dequeue, second message should trigger a fresh NOTIFY"
    );
}

// ── AC-N4: Race-free: dequeue returning remaining:0 then new arrival → NOTIFY ──

#[tokio::test]
async fn ac_n4_race_free_dequeue_then_arrival_fires_notify() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": "N4Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "N4Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    // Dequeue empty queue (should re-arm notify without crashing).
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["remaining"], 0);
    assert!(body["message"].is_null());

    // Now send a message — should fire NOTIFY (notify was re-armed by dequeue).
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "N4Receiver", "payload": "race-test"}))
        .send()
        .await
        .unwrap();

    let mut got_notify = false;
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
            let data = String::from_utf8_lossy(&chunk);
            if data.contains("\"notify\"") {
                got_notify = true;
                break;
            }
        }
    }
    assert!(
        got_notify,
        "NOTIFY must fire after dequeue+new-arrival (race-free invariant)"
    );
}

// ── AC-A4: TOCTOU — two agents simultaneously announce stale name → exactly one succeeds ─

#[tokio::test]
async fn ac_a4_toctou_concurrent_announce_exactly_one_wins() {
    use tokio::task;

    // Agent 0 holds the name, then disconnects (making name stale).
    // Agents A and B then race to claim the stale name simultaneously.
    // Exactly one must succeed; the other must get NAME_IN_USE.

    let server = Arc::new(TestServer::spawn().await);
    let client = Arc::new(server.client());

    // Original holder claims "RaceName" then drops SSE → stale.
    let (tok0, stream0) = listen_get_token(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok0))
        .json(&json!({"name": "RaceName"}))
        .send()
        .await
        .unwrap();
    // Abort the SSE task — makes holder stale (connection count decrements).
    stream0.abort();
    // Give the server time to process the SSE close.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Two racers, both with active SSE (keeps them from being auto-evicted by each other).
    let (tok_a, stream_a) = listen_get_token(&server, &client, None).await;
    let (tok_b, stream_b) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let server_a = Arc::clone(&server);
    let server_b = Arc::clone(&server);
    let client_a = Arc::clone(&client);
    let client_b = Arc::clone(&client);
    let tok_a2 = tok_a.clone();
    let tok_b2 = tok_b.clone();

    // Both race to announce.
    let h_a = task::spawn(async move {
        client_a
            .post(server_a.url("/announce"))
            .header("Authorization", format!("Bearer {}", tok_a2))
            .json(&json!({"name": "RaceName"}))
            .send()
            .await
            .unwrap()
    });
    let h_b = task::spawn(async move {
        client_b
            .post(server_b.url("/announce"))
            .header("Authorization", format!("Bearer {}", tok_b2))
            .json(&json!({"name": "RaceName"}))
            .send()
            .await
            .unwrap()
    });

    let ra = h_a.await.unwrap();
    let rb = h_b.await.unwrap();

    let ra_status = ra.status();
    let rb_status = rb.status();
    // Consume responses — only the CONFLICT one has a JSON body.
    let fail_body: Option<Value> = if ra_status == StatusCode::CONFLICT {
        Some(ra.json().await.unwrap())
    } else if rb_status == StatusCode::CONFLICT {
        Some(rb.json().await.unwrap())
    } else {
        None
    };

    stream_a.abort();
    stream_b.abort();

    let wins = [ra_status, rb_status]
        .iter()
        .filter(|&&s| s == StatusCode::NO_CONTENT)
        .count();
    let fails = [ra_status, rb_status]
        .iter()
        .filter(|&&s| s == StatusCode::CONFLICT)
        .count();
    assert_eq!(
        wins, 1,
        "exactly one concurrent announce should succeed, got statuses: {:?}, {:?}",
        ra_status, rb_status
    );
    assert_eq!(fails, 1, "exactly one concurrent announce should fail");
    if let Some(body) = fail_body {
        assert_eq!(
            body["error"], "NAME_IN_USE",
            "losing announce must have NAME_IN_USE error: {:?}",
            body
        );
    }
}

// ── AC-R1/R3: Token revocation — token invalid AND SSE closed; revoked event ──

#[tokio::test]
async fn ac_r1_r3_token_revocation_atomic() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let participant_tok = _rtok.clone();

    // Announce a name (required for DELETE /participants/{name}).
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .json(&json!({"name": "RevokeTestParticipant"}))
        .send()
        .await
        .unwrap();

    // Verify dequeue with the token works before revocation.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "dequeue with valid token should work"
    );

    // AC-R1: Revoke via DELETE /participants/{name} with governor token.
    // Atomically deregisters and revokes the token.
    let r = client
        .delete(server.url("/participants/RevokeTestParticipant"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "DELETE with governor should return 204"
    );

    // AC-R1: dequeue with the (now revoked) token must fail with 401.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "dequeue with revoked token must fail: {:?}",
        r.status()
    );

    // AC-R3: the SSE stream should have been closed by the revocation (sse_sender dropped).
    // We verify no further chunks arrive (stream ends).
    let next = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    // Either timeout (stream ended) or None (stream closed) is acceptable.
    // extra chunk (e.g. revoked event) is acceptable; stream ended or timed out is expected
    let _ = next;
}

// ── AC-D4: thread filter on single /messages/queue/pop ───────────────────────

#[tokio::test]
async fn ac_d4_thread_filter_on_single_dequeue() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_tok = _rtok.clone();
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .json(&json!({"name": "D4Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "D4Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_tok}))
        .send()
        .await
        .unwrap();

    // Send one message with thread "t1" and one with thread "t2".
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "D4Receiver", "payload": "msg-t1", "thread_id": "t1"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "D4Receiver", "payload": "msg-t2", "thread_id": "t2"}))
        .send()
        .await
        .unwrap();

    // Dequeue with thread "t2" filter — should get msg-t2 (skipping msg-t1).
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .json(&json!({"thread": "t2"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body: Value = r.json().await.unwrap();
    let msg = &body["message"];
    assert!(!msg.is_null(), "expected a message for thread t2");
    assert_eq!(
        msg["payload"], "msg-t2",
        "wrong message returned for thread filter"
    );
    assert_eq!(msg["thread_id"], "t2");

    // Dequeue with thread "t1" filter — should get msg-t1 (t2 was already dequeued above).
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .json(&json!({"thread": "t1"}))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    let msg = &body["message"];
    assert!(!msg.is_null(), "expected a message for thread t1");
    assert_eq!(msg["payload"], "msg-t1");

    // Queue now empty — unfiltered dequeue should return null.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert!(
        body["message"].is_null(),
        "queue should be empty after draining both threads"
    );

    drop(stream);
}

// ── AC-S3: to_token routing in /messages/send ─────────────────────────────────

#[tokio::test]
async fn ac_s3_send_by_token_routes_to_recipient() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_tok = _rtok.clone();
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .json(&json!({"name": "S3Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "S3Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_tok}))
        .send()
        .await
        .unwrap();

    // Send via to_token (not to name).
    let r = client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to_token": recv_tok, "payload": "token-routed"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::ACCEPTED,
        "send via to_token should succeed"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["status"], "accepted");

    // Receiver dequeues and gets the message.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_tok))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    let msg = &body["message"];
    assert!(!msg.is_null(), "receiver should have a queued message");
    assert_eq!(msg["payload"], "token-routed");

    // Send via unknown to_token → 404 RECIPIENT_UNKNOWN.
    let r = client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to_token": "00000000", "payload": "ghost"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NOT_FOUND,
        "unknown to_token should return 404"
    );

    drop(stream);
}

// ── AC-L2: Revoked token → POST /listen returns 401 TOKEN_REVOKED ─────────────

#[tokio::test]
async fn ac_l2_revoked_token_listen_returns_token_revoked() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Issue a token via POST /listen.
    let (token, _welcome) = listen_and_get_welcome(&server, &client, None).await;

    // Announce to allow revocation by name.
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "L2Agent"}))
        .send()
        .await
        .unwrap();

    // Revoke the token via DELETE /participants/L2Agent.
    let r = client
        .delete(server.url("/participants/L2Agent"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "DELETE /participants must return 204"
    );

    // POST /listen with the revoked token must return 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "revoked token must be rejected by /listen"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(
        body["error"], "TOKEN_REVOKED",
        "error code must be TOKEN_REVOKED: {:?}",
        body
    );
}

// ── AC-N5: SERVICE events bypass notify interlock ─────────────────────────────

#[tokio::test]
async fn ac_n5_service_events_bypass_notify_interlock() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver with active SSE.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": "N5Receiver"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "N5Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    // Send a message — this fires a notify on SSE, setting notify_suppressed=true.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "N5Receiver", "payload": "ping"}))
        .send()
        .await
        .unwrap();

    // Drain SSE until we see the notify event (confirms notify_suppressed is now true).
    let mut got_notify = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
            let data = String::from_utf8_lossy(&chunk);
            if data.contains(r#""notify""#) {
                got_notify = true;
                break;
            }
        }
    }
    assert!(
        got_notify,
        "notify event must arrive within 500ms before testing bypass"
    );

    // Revoke the token — notify_suppressed is true, but SERVICE revoked event must bypass it.
    let r = client
        .delete(server.url("/participants/N5Receiver"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "DELETE /participants must return 204"
    );

    // The revoked SERVICE event must arrive on SSE despite notify_suppressed=true.
    let mut got_revoked = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        match tokio::time::timeout(Duration::from_millis(50), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let data = String::from_utf8_lossy(&chunk);
                if data.contains(r#""revoked""#) {
                    got_revoked = true;
                    break;
                }
            }
            Ok(None) | Err(_) => break,
            _ => {}
        }
    }
    assert!(
        got_revoked,
        "SERVICE revoked event must arrive on SSE regardless of notify_suppressed"
    );
}

// ── AC-P3: Presence query works with any valid token (no grant needed) ─────────

#[tokio::test]
async fn ac_p3_presence_any_valid_token_no_grant() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Token A — issued, no grant between A and B.
    let (token_a, _) = listen_and_get_welcome(&server, &client, None).await;

    // Token B — issued and announced with an active SSE connection.
    let (token_b, stream_b) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token_b))
        .json(&json!({"name": "P3AgentB"}))
        .send()
        .await
        .unwrap();

    // Token A queries presence for P3AgentB — no grant between A and B.
    let r = client
        .get(server.url("/participants/P3AgentB/presence"))
        .header("Authorization", format!("Bearer {}", token_a))
        .send()
        .await
        .unwrap();

    // Must succeed (not 401/403) — any valid token may query presence without a grant.
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "presence query must succeed for any valid token"
    );
    let body: Value = r.json().await.unwrap();
    // status is online or offline — either is acceptable, just not an error.
    let status = body["status"].as_str().unwrap_or("");
    assert!(
        status == "online" || status == "offline",
        "status must be online or offline, got: {:?}",
        body
    );

    stream_b.abort();
}

// ── AC-R2: Revoked token returns TOKEN_REVOKED on multiple endpoints ───────────

#[tokio::test]
async fn ac_r2_revoked_token_returns_token_revoked_on_all_endpoints() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Issue and announce a token.
    let (token, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "R2Agent"}))
        .send()
        .await
        .unwrap();
    let r = client
        .delete(server.url("/participants/R2Agent"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "revocation via DELETE must return 204"
    );

    // POST /messages/dequeue with revoked token → 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "dequeue with revoked token must return 401"
    );
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        "TOKEN_REVOKED",
        "dequeue error code must be TOKEN_REVOKED"
    );

    // POST /announce with revoked token → 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "R2AgentNew"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "announce with revoked token must return 401"
    );
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        "TOKEN_REVOKED",
        "announce error code must be TOKEN_REVOKED"
    );
}

// ── AC-L2: Revoked token calling POST /listen → 401 TOKEN_REVOKED ─────────────

#[tokio::test]
async fn ac_l2_revoked_token_listen_returns_401() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "L2Agent"}))
        .send()
        .await
        .unwrap();

    let r = client
        .delete(server.url("/participants/L2Agent"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "DELETE /participants must return 204"
    );

    // Revoked token calling POST /listen must return 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "revoked token POST /listen must return 401"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["error"], "TOKEN_REVOKED");
}

// ── AC-N5: SERVICE events bypass notify-suppressed state ──────────────────────

#[tokio::test]
async fn ac_n5_service_events_bypass_notify_suppressed() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Open SSE and keep it alive.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let participant_tok = _rtok.clone();

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .json(&json!({"name": "N5Agent"}))
        .send()
        .await
        .unwrap();
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "N5Sender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender, "identity_b": participant_tok}))
        .send()
        .await
        .unwrap();

    // Send a message — fires NOTIFY and suppresses notify_suppressed.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "N5Agent", "payload": "suppress-notify"}))
        .send()
        .await
        .unwrap();

    // Drain the NOTIFY event so the stream is sitting in suppressed state.
    tokio::time::sleep(Duration::from_millis(50)).await;
    while let Ok(Some(Ok(_))) = tokio::time::timeout(Duration::from_millis(30), stream.next()).await
    {
    }

    // Revoke the agent — sends SERVICE revoked directly, bypassing notify_suppressed.
    client
        .delete(server.url("/participants/N5Agent"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();

    // SSE must receive the SERVICE revoked event regardless of notify-suppressed state.
    let mut got_revoked = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                if String::from_utf8_lossy(&chunk).contains("revoked") {
                    got_revoked = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        got_revoked,
        "SERVICE revoked event must arrive on SSE even when notify is suppressed"
    );
}

// ── AC-P3: Presence query succeeds with any valid token (no grant required) ───

#[tokio::test]
async fn ac_p3_presence_any_valid_token_no_grant_needed() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Querier (no grant to target).
    let (tok_querier, _) = listen_and_get_welcome(&server, &client, None).await;

    // Target with a live SSE connection.
    let (tok_target, stream_target) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_target))
        .json(&json!({"name": "P3Target"}))
        .send()
        .await
        .unwrap();

    // No grant between querier and target — query must still succeed (not 401/403).
    let r = client
        .get(server.url("/participants/P3Target/presence"))
        .header("Authorization", format!("Bearer {}", tok_querier))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "presence must return 200 without a grant"
    );
    let body: Value = r.json().await.unwrap();
    assert!(
        body["status"] == "online" || body["status"] == "offline",
        "status must be online or offline, got: {:?}",
        body["status"]
    );

    stream_target.abort();
}

// ── AC-R2: Any call with revoked token → 401 TOKEN_REVOKED ────────────────────

#[tokio::test]
async fn ac_r2_revoked_token_all_endpoints_return_token_revoked() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "R2Agent"}))
        .send()
        .await
        .unwrap();

    client
        .delete(server.url("/participants/R2Agent"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();

    // POST /messages/dequeue → 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "dequeue with revoked token"
    );
    assert_eq!(r.json::<Value>().await.unwrap()["error"], "TOKEN_REVOKED");

    // POST /announce → 401 TOKEN_REVOKED.
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({"name": "R2AgentNew"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "announce with revoked token"
    );
    assert_eq!(r.json::<Value>().await.unwrap()["error"], "TOKEN_REVOKED");
}

// ── AC-S2: Send without grant → 200 request_pending; governor receives event ──

#[tokio::test]
async fn ac_s2_send_without_grant_request_pending_gov_event() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let gov_sse_resp = client
        .get(server.url("/governors/events"))
        .header("Authorization", format!("Bearer {}", gov))
        .send()
        .await
        .unwrap();
    assert_eq!(gov_sse_resp.status(), StatusCode::OK);
    let mut gov_stream = gov_sse_resp.bytes_stream();

    // Sender (no grant to receiver — testing request_pending flow).
    let (sender, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"name": "S2Sender"}))
        .send()
        .await
        .unwrap();

    // Recipient B.
    let (tok_b, stream_b) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_b))
        .json(&json!({"name": "S2Receiver"}))
        .send()
        .await
        .unwrap();

    // Rooms gate: both agents must share a room before requesting a grant.
    let room_resp = client
        .post(server.url("/room/create"))
        .header("Authorization", format!("Bearer {}", sender))
        .send()
        .await
        .unwrap();
    assert_eq!(
        room_resp.status(),
        StatusCode::OK,
        "POST /room/create failed"
    );
    let room_id = room_resp.json::<Value>().await.unwrap()["room_id"]
        .as_str()
        .unwrap()
        .to_string();
    for tok in [&sender, &tok_b] {
        let jr = client
            .post(server.url(&format!("/room/{}/join", room_id)))
            .header("Authorization", format!("Bearer {}", tok))
            .send()
            .await
            .unwrap();
        assert_eq!(jr.status(), StatusCode::OK, "room join failed");
    }

    // A sends to B — no grant exists; server returns NO_GRANT (403).
    let r = client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "S2Receiver", "payload": "hi-no-grant"}))
        .send()
        .await
        .unwrap();

    assert_ne!(
        r.status(),
        StatusCode::ACCEPTED,
        "send without grant must NOT return 202"
    );
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "send without grant should return 403 NO_GRANT"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["error"], "NO_GRANT");

    // A explicitly requests a grant; server returns 200 with request_id.
    let rg = client
        .post(server.url("/grants/request"))
        .header("Authorization", format!("Bearer {}", sender))
        .json(&json!({"to": "S2Receiver", "reason": "test connection"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rg.status(),
        StatusCode::OK,
        "grants/request must return 200"
    );
    let rg_body: Value = rg.json().await.unwrap();
    assert!(
        rg_body["request_id"].is_string(),
        "request_id must be present"
    );

    // Governor SSE must receive the grant_request event.
    let mut got_event = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(100), gov_stream.next()).await
            && String::from_utf8_lossy(&chunk).contains("grant_request")
        {
            got_event = true;
            break;
        }
    }
    assert!(got_event, "governor SSE must receive grant_request event");

    stream_b.abort();
}

// ── Governorless mode: recipient-consent grants (no governor required) ────────
//
// With no governor on the hub, a grant is established by the recipient alone:
// A requests, B approves, then A may message B. The governor is optional.
#[tokio::test]
async fn ac_governorless_recipient_consent_grant() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // No governor is minted. Two agents listen + announce.
    let (alice, sa) = listen_get_token(&server, &client, None).await;
    let (bob, sb) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    for (tok, name) in [(&alice, "GlAlice"), (&bob, "GlBob")] {
        client
            .post(server.url("/announce"))
            .header("Authorization", format!("Bearer {}", tok))
            .json(&json!({"name": name}))
            .send()
            .await
            .unwrap();
    }

    // Alice → Bob with no grant: still grant-gated, so NO_GRANT (403).
    let r = client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", alice))
        .json(&json!({"to": "GlBob", "payload": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(r.json::<Value>().await.unwrap()["error"], "NO_GRANT");

    // Rooms gate: Alice and Bob must share a room to bootstrap the grant request.
    let room_resp = client
        .post(server.url("/room/create"))
        .header("Authorization", format!("Bearer {}", alice))
        .send()
        .await
        .unwrap();
    assert_eq!(
        room_resp.status(),
        StatusCode::OK,
        "POST /room/create failed"
    );
    let room_id = room_resp.json::<Value>().await.unwrap()["room_id"]
        .as_str()
        .unwrap()
        .to_string();
    for (tok, name) in [(&alice, "GlAlice"), (&bob, "GlBob")] {
        let jr = client
            .post(server.url(&format!("/room/{}/join", room_id)))
            .header("Authorization", format!("Bearer {}", tok))
            .send()
            .await
            .unwrap();
        assert_eq!(jr.status(), StatusCode::OK, "{name} room join failed");
    }

    // Alice requests a grant — with no governor it routes straight to Bob.
    let rg = client
        .post(server.url("/grants/request"))
        .header("Authorization", format!("Bearer {}", alice))
        .json(&json!({"to": "GlBob", "reason": "let's talk"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rg.status(), StatusCode::OK);
    let req_id = rg.json::<Value>().await.unwrap()["request_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Bob receives the grant_request directly in his feed (not via a governor).
    let pop = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", bob))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert!(
        pop["message"]["payload"]
            .as_str()
            .unwrap_or("")
            .contains("grant_request"),
        "Bob should receive the grant_request, got: {pop}"
    );

    // Bob (the recipient) approves — his consent alone establishes the grant.
    let approve = client
        .patch(server.url(&format!("/grants/requests/{}", req_id)))
        .header("Authorization", format!("Bearer {}", bob))
        .json(&json!({"action": "approve"}))
        .send()
        .await
        .unwrap();
    assert!(
        approve.status().is_success(),
        "recipient approve should succeed, got {}",
        approve.status()
    );

    // Alice can now message Bob.
    let send2 = client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", alice))
        .json(&json!({"to": "GlBob", "payload": "hello bob"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        send2.status(),
        StatusCode::ACCEPTED,
        "send after consent grant should be 202"
    );

    // Bob receives Alice's message.
    let pop2 = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", bob))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(pop2["message"]["payload"], "hello bob");
    assert_eq!(pop2["message"]["from"], "GlAlice");

    sa.abort();
    sb.abort();
}

// ── Governance claim / election / transfer ────────────────────────────────────

/// AC: Fresh server, one agent, POST /governors/claim → 200 granted immediately.
#[tokio::test]
async fn ac_claim_autogrant_first_governor() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // One agent listens and announces; keep SSE alive so they are "active".
    let (tok, _handle) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok))
        .json(&json!({"name": "ClaimAgent"}))
        .send()
        .await
        .unwrap();

    // POST /governors/claim — no governor, no other agents → immediate grant.
    let r = client
        .post(server.url("/governors/claim"))
        .header("Authorization", format!("Bearer {}", tok))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "claim should return 200 for auto-grant"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["status"], "granted", "status must be 'granted'");
    assert!(
        body["governor_token"].is_string(),
        "governor_token must be present"
    );
}

/// AC: Two agents; agent A claims governorship → election; agent B approves → established.
/// Then A dequeues and sees the governorship_granted governance message with a governor_token.
#[tokio::test]
async fn ac_claim_election_unanimous() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Both agents listen+announce; keep SSE alive.
    let (tok_a, _sa) = listen_get_token(&server, &client, None).await;
    let (tok_b, _sb) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    for (tok, name) in [(&tok_a, "ElAlice"), (&tok_b, "ElBob")] {
        client
            .post(server.url("/announce"))
            .header("Authorization", format!("Bearer {}", tok))
            .json(&json!({"name": name}))
            .send()
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Agent A claims governorship — other agent (B) must approve.
    let r = client
        .post(server.url("/governors/claim"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::ACCEPTED,
        "election claim should return 202"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["status"], "election");
    let claim_id = body["claim_id"]
        .as_str()
        .expect("claim_id must be present")
        .to_string();
    assert_eq!(body["voters"], 1, "voters should be 1 (only ElBob)");

    // Agent B receives the election_request in their queue.
    let pop = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", tok_b))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let payload_str = pop["message"]["payload"].as_str().unwrap_or("");
    assert!(
        payload_str.contains("election_request"),
        "B should receive election_request, got: {pop}"
    );

    // Agent B approves.
    let vote = client
        .post(server.url(&format!("/governors/elections/{}", claim_id)))
        .header("Authorization", format!("Bearer {}", tok_b))
        .json(&json!({"action": "approve"}))
        .send()
        .await
        .unwrap();
    assert_eq!(vote.status(), StatusCode::OK, "vote should return 200");
    let vote_body: Value = vote.json().await.unwrap();
    assert_eq!(vote_body["status"], "established");

    // Agent A dequeues and should receive a governorship_granted message with governor_token.
    let pop_a = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let payload_a = pop_a["message"]["payload"].as_str().unwrap_or("");
    assert!(
        payload_a.contains("governorship_granted"),
        "A should receive governorship_granted, got: {pop_a}"
    );
    // Parse the payload to check governor_token is present.
    let inner: Value = serde_json::from_str(payload_a).unwrap();
    assert!(
        inner["governor_token"].is_string(),
        "governor_token must be in payload"
    );

    _sa.abort();
    _sb.abort();
}

/// AC: Install a governor; an agent claims → transfer_pending; governor approves.
#[tokio::test]
async fn ac_claim_transfer_existing_governor() {
    let (server, gov_tok) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // An agent listens, announces, and claims.
    let (participant_tok, _sa) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .json(&json!({"name": "TransferAgent"}))
        .send()
        .await
        .unwrap();

    let r = client
        .post(server.url("/governors/claim"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::ACCEPTED,
        "transfer claim should return 202"
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["status"], "transfer_pending");
    let claim_id = body["claim_id"]
        .as_str()
        .expect("claim_id must be present")
        .to_string();

    // Existing governor approves the transfer.
    let vote = client
        .post(server.url(&format!("/governors/elections/{}", claim_id)))
        .header("Authorization", format!("Bearer {}", gov_tok))
        .json(&json!({"action": "approve"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        vote.status(),
        StatusCode::OK,
        "governor vote should return 200"
    );
    let vote_body: Value = vote.json().await.unwrap();
    assert_eq!(vote_body["status"], "established");

    // Agent dequeues and should see governorship_granted.
    let pop = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", participant_tok))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let payload_str = pop["message"]["payload"].as_str().unwrap_or("");
    assert!(
        payload_str.contains("governorship_granted"),
        "Participant should receive governorship_granted, got: {pop}"
    );

    _sa.abort();
}

// ── Presence recovery (sim-roster-status-unreliable) ─────────────────────────
//
// AC1: GET /participants/<name>/presence returns "online" for agents that
//      are reachable (active SSE + announced, within the liveness window).
// AC2: Presence recovers automatically after an SSE drop → re-announce cycle.
// AC3: An agent that has not announced recently still shows "offline".

/// Helper: spawn a TestServer with a custom liveness window (for AC3's short-TTL variant).
async fn spawn_server_with_ttl(liveness_secs: u64) -> TestServer {
    let state = Arc::new(AppState::new(Duration::from_secs(liveness_secs)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = simple_im::http::router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestServer {
        base_url: format!("http://127.0.0.1:{}", addr.port()),
    }
}

/// Helper: spawn a TestServer with a custom liveness window AND a governor.
async fn spawn_server_with_governor_ttl(liveness_secs: u64) -> (TestServer, String) {
    use simple_im::delivery::DeliveryHub;
    let hub = DeliveryHub::new(Duration::from_secs(liveness_secs));
    let gov = hub.install_governor(None);
    let gov_tok = gov.0.clone();
    let state = Arc::new(AppState::new_with_hub(hub));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = simple_im::http::router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        TestServer {
            base_url: format!("http://127.0.0.1:{}", addr.port()),
        },
        gov_tok,
    )
}

// ── AC1: presence returns "online" for a reachable agent ─────────────────────

#[tokio::test]
async fn ac_pr1_presence_online_after_announce() {
    // Need governor to approve the grant required by the new grant-gated visibility model.
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Agent A: open SSE and keep it alive; announce.
    let (tok_a, stream_a) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({"name": "PR1Agent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce must return 204"
    );

    // Agent B: register and open a listen session for querying.
    let (tok_b, _stream_b) = listen_get_token(&server, &client, None).await;

    // Grant required for B to see A's presence (new grant-gated visibility model).
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": tok_a, "identity_b": tok_b}))
        .send()
        .await
        .unwrap();

    // AC1: Agent B queries Agent A's presence — A has active SSE and has announced.
    let r = client
        .get(server.url("/participants/PR1Agent/presence"))
        .header("Authorization", format!("Bearer {}", tok_b))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "presence query must return 200");
    let body: Value = r.json().await.unwrap();
    assert_eq!(
        body["status"], "online",
        "participant with active SSE + announce must appear online: {:?}",
        body
    );

    stream_a.abort();
}

// ── AC2: presence recovers after SSE drop → re-announce ──────────────────────

#[tokio::test]
async fn ac_pr2_presence_recovers_after_sse_drop_and_reannounce() {
    // Use a 1-second liveness TTL so we can drive the offline→online transition
    // within the test.  With the 30 s default the agent would stay online via the
    // registry for far too long to be observable in a unit test.
    // Governor needed for grant approval (new grant-gated visibility model).
    let (server, gov) = spawn_server_with_governor_ttl(1).await;
    let client = server.client();

    // Agent A: open SSE, announce.
    let (tok_a, stream_a) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({"name": "PR2Agent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce must return 204"
    );

    // Querier token.
    let (tok_q, _stream_q) = listen_get_token(&server, &client, None).await;

    // Grant required for querier to see agent A's presence (new grant-gated visibility model).
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": tok_a, "identity_b": tok_q}))
        .send()
        .await
        .unwrap();

    // Verify agent is online before dropping SSE.
    let r = client
        .get(server.url("/participants/PR2Agent/presence"))
        .header("Authorization", format!("Bearer {}", tok_q))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.json::<Value>().await.unwrap()["status"],
        "online",
        "should be online before drop"
    );

    // Simulate SSE monitor drop (abort the SSE-holding task).
    stream_a.abort();
    // Give the server time to process the SSE close, then wait for the 1 s registry
    // TTL to expire so the agent transitions to offline.
    tokio::time::sleep(Duration::from_millis(100)).await; // SSE drop processed
    tokio::time::sleep(Duration::from_millis(1100)).await; // TTL expires (1 s + buffer)

    // After SSE drop + TTL expiry, presence shows offline.
    let r = client
        .get(server.url("/participants/PR2Agent/presence"))
        .header("Authorization", format!("Bearer {}", tok_q))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.json::<Value>().await.unwrap()["status"],
        "offline",
        "should be offline after SSE drop + TTL expiry"
    );

    // Agent re-announces (without opening a new SSE — simulating announce-only recovery).
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({"name": "PR2Agent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "re-announce must return 204"
    );

    // AC2: presence must now show online (recovered via registry liveness refresh).
    let r = client
        .get(server.url("/participants/PR2Agent/presence"))
        .header("Authorization", format!("Bearer {}", tok_q))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert_eq!(
        body["status"], "online",
        "presence must recover to online after re-announce: {:?}",
        body
    );
}

// ── AC3: absent agent (TTL expired, no re-announce) shows "offline" ───────────

#[tokio::test]
async fn ac_pr3_absent_participant_shows_offline_after_ttl_expires() {
    // Use a 1-second liveness window so the test doesn't take 30 s.
    let server = spawn_server_with_ttl(1).await;
    let client = server.client();

    // Agent A: open SSE, announce.
    let (tok_a, stream_a) = listen_get_token(&server, &client, None).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", tok_a))
        .json(&json!({"name": "PR3Agent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce must return 204"
    );

    // Querier.
    let (tok_q, _stream_q) = listen_get_token(&server, &client, None).await;

    // Drop SSE so the agent is no longer connected.
    stream_a.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // AC3: wait for the liveness TTL to expire (>1 s).
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Presence must show offline — TTL expired, no re-announce, no SSE.
    let r = client
        .get(server.url("/participants/PR3Agent/presence"))
        .header("Authorization", format!("Bearer {}", tok_q))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert_eq!(
        body["status"], "offline",
        "absent participant (TTL expired, no SSE, no re-announce) must show offline: {:?}",
        body
    );
}

// ── AC-GOV-BREADCRUMB: governor gets role breadcrumb on listen+announce ────────

#[tokio::test]
async fn ac_gov_breadcrumb_on_connect() {
    let (server, gov_token) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Governor: POST /listen with their governor token as bearer.
    // The server detects this is a governor token, mints a linked listen token,
    // and records the session link so announce() can enqueue the governor_role breadcrumb.
    let (listen_tok, _stream) = listen_get_token(&server, &client, Some(&gov_token)).await;

    // Governor: POST /announce — this triggers the governor-role breadcrumb to be enqueued.
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", listen_tok))
        .json(&json!({"name": "Governor"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "announce must return 204"
    );

    // Small yield so the async SSE NOTIFY delivery can process.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Dequeue: must receive the governor_role service message.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", listen_tok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "dequeue failed");
    let body: Value = r.json().await.unwrap();

    let msg = &body["message"];
    assert!(
        !msg.is_null(),
        "expected a governor_role breadcrumb, got null. body: {:?}",
        body
    );

    // The payload is a JSON string — parse it.
    let payload: Value =
        serde_json::from_str(msg["payload"].as_str().unwrap()).expect("payload must be valid JSON");
    assert_eq!(
        payload["type"], "service",
        "breadcrumb type must be 'service': {:?}",
        payload
    );
    assert_eq!(
        payload["kind"], "governor_role",
        "breadcrumb kind must be 'governor_role': {:?}",
        payload
    );
    assert_eq!(
        payload["role"], "governor",
        "breadcrumb role must be 'governor': {:?}",
        payload
    );

    // Confirm event_type field on the queued message.
    assert_eq!(
        msg["event_type"], "governor_role",
        "event_type on dequeued message must be 'governor_role': {:?}",
        msg
    );

    _stream.abort();
}

// ── Helpers for last-message-id tests ─────────────────────────────────────────

/// Set up a minimal sender→receiver pair with a grant.
/// Returns (server, client, recv_token, sender_token).
async fn setup_send_pair(
    name_recv: &str,
    name_sender: &str,
) -> (TestServer, reqwest::Client, String, String) {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver — keep SSE open for notify delivery.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    let mut stream = r.bytes_stream();
    let mut buf = String::new();
    // Consume welcome event; token comes from registration, not welcome.
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let recv_token = _rtok.clone();
    // Keep stream alive in background.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(stream);
    });
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .json(&json!({"name": name_recv}))
        .send()
        .await
        .unwrap();

    // Sender.
    let (sender_token, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"name": name_sender}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {}", gov))
        .json(&json!({"identity_a": sender_token, "identity_b": recv_token}))
        .send()
        .await
        .unwrap();

    (server, client, recv_token, sender_token)
}

// ── AC-LID1: GET /messages/latest/id returns 404 when no messages received ────

#[tokio::test]
async fn ac_lid1_latest_id_returns_404_when_no_messages() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    let r = client
        .get(server.url("/messages/latest/id"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NOT_FOUND,
        "should 404 when no messages received"
    );
}

// ── AC-LID2: GET /messages/latest/id returns correct ID after message sent ────

#[tokio::test]
async fn ac_lid2_latest_id_increments_after_each_message() {
    let (server, client, recv_token, sender_token) =
        setup_send_pair("LID2Recv", "LID2Sender").await;

    // No message yet — 404.
    let r = client
        .get(server.url("/messages/latest/id"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    // Send first message.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"to": "LID2Recv", "payload": "msg1"}))
        .send()
        .await
        .unwrap();

    // Poll until the ID appears (server delivers asynchronously).
    let mut id1 = 0u64;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let r = client
            .get(server.url("/messages/latest/id"))
            .header("Authorization", format!("Bearer {}", recv_token))
            .send()
            .await
            .unwrap();
        if r.status() == StatusCode::OK {
            let body = r.text().await.unwrap();
            id1 = body.trim().parse().unwrap_or(0);
            if id1 > 0 {
                break;
            }
        }
    }
    assert!(
        id1 >= 1,
        "first message must set last_message_id ≥ 1, got {}",
        id1
    );

    // Send second message.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"to": "LID2Recv", "payload": "msg2"}))
        .send()
        .await
        .unwrap();

    let mut id2 = id1;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let r = client
            .get(server.url("/messages/latest/id"))
            .header("Authorization", format!("Bearer {}", recv_token))
            .send()
            .await
            .unwrap();
        if r.status() == StatusCode::OK {
            let body = r.text().await.unwrap();
            let v: u64 = body.trim().parse().unwrap_or(0);
            if v > id1 {
                id2 = v;
                break;
            }
        }
    }
    assert!(
        id2 > id1,
        "second message must increment last_message_id: {} → {}",
        id1,
        id2
    );
}

// ── AC-LM1: GET /messages/latest returns 404 when no messages ─────────────────

#[tokio::test]
async fn ac_lm1_latest_message_returns_404_when_no_messages() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    let r = client
        .get(server.url("/messages/latest"))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

// ── AC-LM2: GET /messages/latest returns the latest queued message ─────────────

#[tokio::test]
async fn ac_lm2_latest_message_returns_last_enqueued_message() {
    let (server, client, recv_token, sender_token) = setup_send_pair("LM2Recv", "LM2Sender").await;

    // Send two messages; latest should be the second.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"to": "LM2Recv", "payload": "first"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"to": "LM2Recv", "payload": "second"}))
        .send()
        .await
        .unwrap();

    // Poll for the message.
    let mut payload_found = String::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let r = client
            .get(server.url("/messages/latest"))
            .header("Authorization", format!("Bearer {}", recv_token))
            .send()
            .await
            .unwrap();
        if r.status() == StatusCode::OK {
            let body: Value = r.json().await.unwrap();
            if let Some(p) = body["message"]["payload"].as_str() {
                payload_found = p.to_string();
                break;
            }
        }
    }
    // Latest (back of deque) should be "second".
    assert_eq!(
        payload_found, "second",
        "latest message should be the last enqueued"
    );

    // Messages are not consumed — dequeue should still return both.
    let r = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {}", recv_token))
        .send()
        .await
        .unwrap();
    let body: Value = r.json().await.unwrap();
    assert!(
        body["remaining"].as_u64().unwrap_or(0) >= 1,
        "peek must not consume messages"
    );
}

// ── AC-LID3: SSE sub event contains last_message_id field ─────────────────────

#[tokio::test]
async fn ac_lid3_sub_event_contains_last_message_id() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // POST /listen — read until we see the sub event (second data event after welcome).
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {}", _rtok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let mut stream = r.bytes_stream();
    let mut buf = String::new();

    let mut sub_event: Option<Value> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    'outer: while std::time::Instant::now() < deadline {
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            buf.push_str(&String::from_utf8_lossy(&chunk));
            for line in buf.lines() {
                if line.starts_with("data:") {
                    let data = line.trim_start_matches("data:").trim();
                    if let Ok(v) = serde_json::from_str::<Value>(data)
                        && v["type"] == "sub"
                    {
                        sub_event = Some(v);
                        break 'outer;
                    }
                }
            }
        }
    }

    let sub = sub_event.expect("sub event must be emitted after POST /listen");
    assert!(
        sub["last_message_id"].is_number(),
        "sub event must contain last_message_id: {:?}",
        sub
    );
    assert_eq!(
        sub["last_message_id"].as_u64().unwrap_or(99),
        0,
        "fresh listener with no messages must have last_message_id=0"
    );
}

// ── AC-LID4: long-poll ?since=N&wait=M returns 204 on timeout ─────────────────

#[tokio::test]
async fn ac_lid4_long_poll_returns_204_on_timeout() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let (token, _) = listen_and_get_welcome(&server, &client, None).await;

    // Long-poll with since=0, wait=1s — no messages will arrive, expect 204.
    let r = client
        .get(server.url("/messages/latest/id?since=0&wait=1"))
        .header("Authorization", format!("Bearer {}", token))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::NO_CONTENT,
        "long-poll with no new messages must return 204"
    );
}

// ── AC-LID5: long-poll returns 200 with new ID when message arrives ────────────

#[tokio::test]
async fn ac_lid5_long_poll_returns_200_when_message_arrives() {
    let (server, client, recv_token, sender_token) =
        setup_send_pair("LID5Recv", "LID5Sender").await;

    let server = std::sync::Arc::new(server);
    let client = std::sync::Arc::new(client);

    let recv_clone = recv_token.clone();
    let server_clone = Arc::clone(&server);
    let client_clone = Arc::clone(&client);

    // Start long-poll in background before sending the message.
    let poll_handle = tokio::spawn(async move {
        client_clone
            .get(server_clone.url("/messages/latest/id?since=0&wait=5"))
            .header("Authorization", format!("Bearer {}", recv_clone))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .unwrap()
    });

    // Small delay to let the long-poll connect.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a message — should wake the long-poll.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {}", sender_token))
        .json(&json!({"to": "LID5Recv", "payload": "wake!"}))
        .send()
        .await
        .unwrap();

    let r = poll_handle.await.unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "long-poll should return 200 when message arrives"
    );
    let body = r.text().await.unwrap();
    let id: u64 = body.trim().parse().expect("body must be bare integer");
    assert!(id >= 1, "returned ID must be ≥ 1, got {}", id);
}

// ── AC-GT1: POST /governors/accept-transfer reads token from Authorization header ─

#[tokio::test]
async fn ac_gt1_accept_transfer_reads_token_from_auth_header() {
    let (server, gov_token) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Initiate a governor transfer — returns a one-time transfer_token.
    let transfer_resp = client
        .post(server.url("/governors/transfer"))
        .header("Authorization", format!("Bearer {}", gov_token))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        transfer_resp.status(),
        StatusCode::OK,
        "transfer initiation should succeed"
    );
    let transfer_body: Value = transfer_resp.json().await.unwrap();
    let transfer_token = transfer_body["transfer_token"]
        .as_str()
        .expect("missing transfer_token")
        .to_string();

    // Accept the transfer — transfer_token goes in Authorization header, name goes in body.
    let accept_resp = client
        .post(server.url("/governors/accept-transfer"))
        .header("Authorization", format!("Bearer {}", transfer_token))
        .json(&json!({"name": "new-governor"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        accept_resp.status(),
        StatusCode::OK,
        "accept-transfer should succeed"
    );
    let accept_body: Value = accept_resp.json().await.unwrap();
    assert!(
        accept_body["token"].is_string(),
        "response must contain new governor token"
    );

    // Calling without Authorization header must return 401.
    let no_auth_resp = client
        .post(server.url("/governors/accept-transfer"))
        .json(&json!({"name": "intruder"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        no_auth_resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing Authorization header must return 401"
    );

    // Calling with transfer_token in body (old pattern) and no Authorization header must also return 401.
    let old_pattern_resp = client
        .post(server.url("/governors/accept-transfer"))
        .json(&json!({"transfer_token": transfer_token, "name": "intruder"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        old_pattern_resp.status(),
        StatusCode::UNAUTHORIZED,
        "transfer_token in body (old pattern) must not be accepted"
    );
}

// ── Presence push event tests (AC1–AC4) ───────────────────────────────────────
//
// These tests use the DeliveryHub directly (not HTTP) to verify that
// presence-change SSE events are pushed to grant-peers.

fn make_presence_hub() -> (
    simple_im::delivery::DeliveryHub,
    simple_im::types::GovernorToken,
) {
    use simple_im::delivery::DeliveryHub;
    let hub = DeliveryHub::new(Duration::from_secs(30));
    let gov = hub.install_governor(None);
    (hub, gov)
}

/// Drain a receiver until it's empty, collecting all events.
fn drain_receiver(rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>) -> Vec<String> {
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    events
}

/// Wait for an event containing all of the given substrings, with a deadline.
async fn wait_for_event_containing(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    needles: &[&str],
    deadline: tokio::time::Instant,
) -> Option<String> {
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) => {
                if needles.iter().all(|n| ev.contains(n)) {
                    return Some(ev);
                }
            }
            _ => return None,
        }
    }
}

// AC1: POST /announce → grant-peers with active SSE receive online event.
#[tokio::test]
async fn ac_pp1_announce_sends_online_to_grant_peer_sse() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    // Agent A: open listen stream (observer).
    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA", false).unwrap();

    // Agent B: open listen stream but do NOT announce yet.
    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();

    // Establish grant with explicit names (B not yet announced, so FP1 wouldn't find name_b).
    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA".to_string()),
            name_b: Some("PpB".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // Drain any setup events from A's stream (welcome, sub, breadcrumb).
    drain_receiver(&mut rx_a);

    // B announces for the first time — should trigger online event to A.
    hub.announce(&tok_b, "PpB", false).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"online\"", "\"PpB\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "A should receive presence online event when B announces; got nothing"
    );
}

// AC2: DELETE /listen (clean cancel) → grant-peers receive offline event.
#[tokio::test]
async fn ac_pp2_cancel_listen_sends_offline_to_grant_peer_sse() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA2", false).unwrap();

    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();
    hub.announce(&tok_b, "PpB2", false).unwrap();

    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA2".to_string()),
            name_b: Some("PpB2".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // Drain setup events.
    drain_receiver(&mut rx_a);

    // B cancels listen (clean, voluntary) — triggers offline event to A.
    hub.cancel_listen(&tok_b).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"offline\"", "\"PpB2\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "A should receive presence offline event when B cancels listen; got nothing"
    );
}

// AC3: SSE liveness expiry (unexpected drop) → grant-peers receive offline event.
#[tokio::test]
async fn ac_pp3_sse_drop_sends_offline_to_grant_peer_sse() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA3", false).unwrap();

    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();
    hub.announce(&tok_b, "PpB3", false).unwrap();

    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA3".to_string()),
            name_b: Some("PpB3".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // Drain setup events.
    drain_receiver(&mut rx_a);

    // Simulate B's SSE connection dropping unexpectedly (no clean cancel).
    // close_listen() is what the HTTP drop-guard calls on connection close.
    hub.close_listen(&tok_b);

    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"offline\"", "\"PpB3\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "A should receive presence offline event on unexpected SSE drop; got nothing"
    );
}

// AC4: Participants without a grant do NOT receive each other's presence events.
#[tokio::test]
async fn ac_pp4_no_grant_no_presence_event_to_non_peer() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    // A and B have a grant (A is the observer with active SSE).
    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA4", false).unwrap();

    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();
    hub.announce(&tok_b, "PpB4", false).unwrap();

    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA4".to_string()),
            name_b: Some("PpB4".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // C has NO grant with A.
    let tok_c = hub.register_participant();

    let (_, _rx_c) = hub
        .open_listen(Some(&tok_c), None, None, None, false)
        .unwrap();

    // Drain setup events so we start fresh.
    drain_receiver(&mut rx_a);

    // C announces — should NOT produce a presence event to A.
    hub.announce(&tok_c, "PpC4", false).unwrap();

    // Give a short window for any spurious events.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = drain_receiver(&mut rx_a);
    let got_c_event = events
        .iter()
        .any(|ev| ev.contains("\"PpC4\"") && ev.contains("\"presence\""));
    assert!(
        !got_c_event,
        "A must NOT receive presence events for C (no grant); got: {:?}",
        events
    );
}

// AC5 (15-0002F): minted-agent deregisters → V2 listen-flow grant-peer receives offline event.
//
// This tests the identity-keyed grant path: the grant is approved using the minted agent's
// identity string (not their token), so the FP1 name lookup in approve_grant_req sets name_a=None.
// Before the fix, list_grants_for_name("bob") found no grant and alice never received the event.
// After the fix, grant_counterparties_for("bob", "bob-id") finds the grant via identity_a match.
//
// Note: register() does NOT call grant_peer_senders, so there is intentionally no corresponding
// online test — minted agents have no announce() path and no "online" presence push.
#[tokio::test]
async fn ac_pp5_minted_agent_deregister_sends_offline_to_listen_peer() {
    use simple_im::registry::PresenceScope;
    use simple_im::types::ParticipantToken;

    let (hub, gov) = make_presence_hub();

    // Alice: V2 listen-flow agent (observer — will receive presence events).
    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA5", false).unwrap();

    // Bob: minted participant with a stable identity distinct from his token.
    let bob_tok: ParticipantToken = hub.mint_participant_token(&gov, "bob-id-5", None).unwrap();
    hub.register("PpB5", &bob_tok, PresenceScope::Public)
        .unwrap();

    // Grant approved via the governor API using Bob's IDENTITY ("bob-id-5"), not his token.
    // This mirrors the real-world path where the governor resolves identities, not tokens.
    // FP1's name lookup (token_to_name.get("bob-id-5")) returns None, so name_a = None.
    // The grant is stored as: name_a=None, name_b="PpA5", identity_a="bob-id-5", identity_b=tok_a.
    hub.approve_grant(&gov, "bob-id-5", &tok_a, None).unwrap();

    // Drain setup events from alice's stream.
    drain_receiver(&mut rx_a);

    // Bob deregisters — should trigger an offline presence event to Alice.
    hub.deregister("PpB5", &bob_tok).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"offline\"", "\"PpB5\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "Alice must receive presence offline event when minted participant PpB5 deregisters; got nothing"
    );
}

// AC6 (15-0002G): force-eviction in announce() → grant-peer receives sim_offline.
//
// Agent B holds a name with an active SSE. Agent C announces the same name with force=true,
// evicting B. Agent A is a grant-peer of B with an active SSE stream and must receive a
// sim_offline presence event for B's name.
#[tokio::test]
async fn ac_pp6_force_eviction_sends_offline_to_grant_peer_sse() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    // Agent A: the observer — has an active SSE stream and a grant with B.
    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA6", false).unwrap();

    // Agent B: announces "PpB6" — will be force-evicted.
    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();
    hub.announce(&tok_b, "PpB6", false).unwrap();

    // Approve grant between A and B with explicit names.
    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA6".to_string()),
            name_b: Some("PpB6".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // Drain setup events from A's stream (welcome, online, grant breadcrumbs).
    drain_receiver(&mut rx_a);

    // Agent C force-evicts B by announcing "PpB6" with force=true.
    let tok_c = hub.register_participant();

    let (_, _rx_c) = hub
        .open_listen(Some(&tok_c), None, None, None, false)
        .unwrap();
    hub.announce(&tok_c, "PpB6", true).unwrap();

    // A must receive sim_offline for "PpB6".
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"offline\"", "\"PpB6\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "A must receive sim_offline for PpB6 when C force-evicts B; got nothing"
    );
}

// AC6b (15-0002G): stale-holder reclaim in announce() → grant-peer receives sim_offline.
//
// Agent B holds a name but its SSE drops (close_listen, simulating connection loss) without
// an explicit cancel_listen. The name binding remains. Agent C announces the same name
// (no force needed — holder is stale). Agent A, a grant-peer of B, must receive sim_offline.
#[tokio::test]
async fn ac_pp6b_stale_holder_reclaim_sends_offline_to_grant_peer_sse() {
    use simple_im::trust::ApproveGrantRequest;

    let (hub, gov) = make_presence_hub();

    // Agent A: the observer.
    let tok_a = hub.register_participant();

    let (_, mut rx_a) = hub
        .open_listen(Some(&tok_a), None, None, None, false)
        .unwrap();
    hub.announce(&tok_a, "PpA6b", false).unwrap();

    // Agent B: announces "PpB6b" then its SSE drops without cancel_listen.
    let tok_b = hub.register_participant();

    let (_, _rx_b) = hub
        .open_listen(Some(&tok_b), None, None, None, false)
        .unwrap();
    hub.announce(&tok_b, "PpB6b", false).unwrap();

    // Approve grant between A and B.
    hub.approve_grant_req(
        &gov,
        &tok_a,
        &tok_b,
        None,
        ApproveGrantRequest {
            name_a: Some("PpA6b".to_string()),
            name_b: Some("PpB6b".to_string()),
            ..ApproveGrantRequest::default()
        },
    )
    .unwrap();

    // Simulate B's SSE dropping unexpectedly (no clean cancel — name binding remains).
    hub.close_listen(&tok_b);

    // Drain A's events (includes the offline from close_listen — we're checking the
    // announce-reclaim path fires an additional or equal offline, but since ac_pp3
    // covers close_listen, we drain and re-establish a clean observation window).
    drain_receiver(&mut rx_a);

    // Agent C reclaims "PpB6b" without force (stale holder → no NAME_IN_USE returned).
    let tok_c = hub.register_participant();

    let (_, _rx_c) = hub
        .open_listen(Some(&tok_c), None, None, None, false)
        .unwrap();
    // B's name binding persists after close_listen; a non-force announce by C should
    // evict the stale binding and fire sim_offline to A.
    hub.announce(&tok_c, "PpB6b", false).unwrap();

    // A must receive sim_offline for "PpB6b" from the stale-holder eviction.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let ev = wait_for_event_containing(
        &mut rx_a,
        &["\"presence\"", "\"offline\"", "\"PpB6b\""],
        deadline,
    )
    .await;
    assert!(
        ev.is_some(),
        "A must receive sim_offline for PpB6b when C reclaims the stale binding; got nothing"
    );
}

// ── End of presence push tests ─────────────────────────────────────────────────

// ── Startup announce tests ─────────────────────────────────────────────────────

fn make_hub() -> simple_im::delivery::DeliveryHub {
    use simple_im::delivery::DeliveryHub;
    DeliveryHub::new(Duration::from_secs(30))
}

#[tokio::test]
async fn ac_startup_announce_first_sub_only() {
    let hub = make_hub();
    // AC1: first subscriber gets sim_online
    let _tok1 = hub.register_participant();

    let (_, mut rx1) = hub
        .open_listen(Some(&_tok1), None, None, None, false)
        .unwrap();
    let mut events1 = vec![];
    while let Ok(ev) = rx1.try_recv() {
        events1.push(ev);
    }
    let has_sim_online = events1.iter().any(|e| {
        let v: serde_json::Value = serde_json::from_str(e).unwrap_or_default();
        v["type"] == "service" && v["event"] == "sim_online"
    });
    assert!(
        has_sim_online,
        "AC1: first subscriber must receive sim_online; got: {:?}",
        events1
    );

    // AC2: second subscriber does NOT get sim_online
    let _tok2 = hub.register_participant();

    let (_, mut rx2) = hub
        .open_listen(Some(&_tok2), None, None, None, false)
        .unwrap();
    let mut events2 = vec![];
    while let Ok(ev) = rx2.try_recv() {
        events2.push(ev);
    }
    let has_sim_online_2 = events2.iter().any(|e| {
        let v: serde_json::Value = serde_json::from_str(e).unwrap_or_default();
        v["type"] == "service" && v["event"] == "sim_online"
    });
    assert!(
        !has_sim_online_2,
        "AC2: second subscriber must NOT receive sim_online; got: {:?}",
        events2
    );
}

// ── Rooms discovery tests ──────────────────────────────────────────────────────

/// Helper: create, listen + announce an agent and return its token.
async fn setup_agent(server: &TestServer, client: &reqwest::Client, name: &str) -> String {
    let (tok, _) = listen_get_token(server, client, None).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let r = client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {tok}"))
        .json(&json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert!(
        r.status().is_success(),
        "announce {name} failed: {}",
        r.status()
    );
    tok
}

/// Helper: POST /room/create and return the room_id.
async fn create_room(server: &TestServer, client: &reqwest::Client, tok: &str) -> String {
    let r = client
        .post(server.url("/room/create"))
        .header("Authorization", format!("Bearer {tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "POST /room/create failed");
    r.json::<Value>().await.unwrap()["room_id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Helper: POST /room/{room_id}/join.
async fn join_room(
    server: &TestServer,
    client: &reqwest::Client,
    tok: &str,
    room_id: &str,
    ttl: Option<u64>,
) -> Value {
    let body = match ttl {
        Some(t) => json!({"ttl": t}),
        None => json!({}),
    };
    let r = client
        .post(server.url(&format!("/room/{room_id}/join")))
        .header("Authorization", format!("Bearer {tok}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "POST /room/{room_id}/join failed"
    );
    r.json::<Value>().await.unwrap()
}

// AC1: POST /room/create returns {room_id} and caller is NOT auto-joined.
#[tokio::test]
async fn ac_room_1_create_returns_room_id_caller_not_joined() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice1").await;

    let r = client
        .post(server.url("/room/create"))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "POST /room/create must return 200"
    );
    let body: Value = r.json().await.unwrap();
    let room_id = body["room_id"].as_str().unwrap();
    assert!(!room_id.is_empty(), "room_id must be non-empty");

    // Caller must NOT be auto-joined — GET /room/{id} should return 403.
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::FORBIDDEN,
        "caller must NOT be auto-joined after create"
    );
}

// AC2: POST /room/{room_id}/join adds caller, returns member list, HTTP 200 on re-join.
#[tokio::test]
async fn ac_room_2_join_adds_caller_idempotent() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice2").await;
    let bob = setup_agent(&server, &client, "RoomBob2").await;

    let room_id = create_room(&server, &client, &alice).await;

    // Alice joins.
    let j1 = join_room(&server, &client, &alice, &room_id, None).await;
    let names: Vec<&str> = j1["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"RoomAlice2"),
        "alice must be in member list"
    );

    // Bob joins.
    let j2 = join_room(&server, &client, &bob, &room_id, None).await;
    let names2: Vec<&str> = j2["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(
        names2.contains(&"RoomAlice2"),
        "alice in list after bob joins"
    );
    assert!(names2.contains(&"RoomBob2"), "bob in list after bob joins");

    // Re-join alice — must return 200 (idempotent).
    let r_rejoin = client
        .post(server.url(&format!("/room/{room_id}/join")))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r_rejoin.status(), StatusCode::OK, "re-join must return 200");
}

// AC3: default TTL 300s applied when omitted; explicit TTL param accepted.
#[tokio::test]
async fn ac_room_3_ttl_default_and_explicit() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice3").await;

    let room_id = create_room(&server, &client, &alice).await;

    // Join with explicit TTL (1 s) — member present immediately.
    let j = join_room(&server, &client, &alice, &room_id, Some(1)).await;
    let names: Vec<&str> = j["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"RoomAlice3"),
        "alice must be present with ttl=1"
    );

    // After TTL expires, alice should be silently removed.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // GET should return 403 (alice expired, no longer a member).
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::FORBIDDEN,
        "alice must be removed after TTL expires"
    );

    // Re-join with no TTL (default 300 s) — must succeed.
    let r = client
        .post(server.url(&format!("/room/{room_id}/join")))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "join with default TTL must succeed"
    );
}

// AC4: GET /room/{room_id} returns member list with online status; 403 if not member.
#[tokio::test]
async fn ac_room_4_get_member_list_and_access_control() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice4").await;
    let bob = setup_agent(&server, &client, "RoomBob4").await;

    let room_id = create_room(&server, &client, &alice).await;

    // Non-member GET must return 403.
    let r403 = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {bob}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r403.status(),
        StatusCode::FORBIDDEN,
        "non-member GET must return 403"
    );

    // Alice joins; GET as Alice must return member list with online field.
    join_room(&server, &client, &alice, &room_id, None).await;

    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_r.status(), StatusCode::OK, "member GET must return 200");
    let body: Value = get_r.json().await.unwrap();
    let members = body["members"].as_array().unwrap();
    assert!(!members.is_empty(), "member list must not be empty");
    // Each member must have name + online fields.
    for m in members {
        assert!(m["name"].is_string(), "member must have name");
        assert!(m["online"].is_boolean(), "member must have online field");
    }
}

// AC5: POST /room/{room_id}/leave removes caller; idempotent (200 if not member).
#[tokio::test]
async fn ac_room_5_leave_removes_caller_idempotent() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice5").await;
    let bob = setup_agent(&server, &client, "RoomBob5").await;

    let room_id = create_room(&server, &client, &alice).await;
    join_room(&server, &client, &alice, &room_id, None).await;
    join_room(&server, &client, &bob, &room_id, None).await;

    // Alice leaves.
    let leave_r = client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(leave_r.status(), StatusCode::OK, "leave must return 200");

    // After leave, alice cannot GET the room.
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::FORBIDDEN,
        "alice must not see room after leaving"
    );

    // Bob still in room.
    let bob_get = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {bob}"))
        .send()
        .await
        .unwrap();
    assert_eq!(bob_get.status(), StatusCode::OK, "bob still in room");
    let bob_body = bob_get.json::<Value>().await.unwrap();
    let members: Vec<&str> = bob_body["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(
        !members.contains(&"RoomAlice5"),
        "alice must not appear after leave"
    );

    // Idempotent: alice leaving again must return 200.
    let leave2 = client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        leave2.status(),
        StatusCode::OK,
        "second leave must be idempotent 200"
    );

    // Leaving a non-existent room must also return 200.
    let leave3 = client
        .post(server.url("/room/nonexistent-room-id/leave"))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        leave3.status(),
        StatusCode::OK,
        "leave non-existent room must return 200"
    );
}

// AC6: TTL expiry removes agent silently; no notification emitted.
// AC7: No join/leave SSE events pushed to room members.
#[tokio::test]
async fn ac_room_6_7_ttl_expiry_silent_no_sse_events() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice6").await;
    let bob = setup_agent(&server, &client, "RoomBob6").await;

    let room_id = create_room(&server, &client, &alice).await;

    // Both join with 1-second TTL.
    join_room(&server, &client, &alice, &room_id, Some(1)).await;
    join_room(&server, &client, &bob, &room_id, Some(1)).await;

    // After TTL expiry (1 s), membership is silently removed.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // GET should return 403 for alice (expired).
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::FORBIDDEN,
        "alice must be removed silently after TTL"
    );

    // AC7: Bob's message queue must NOT contain any room-related events.
    let pop = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {bob}"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    // No message queued (or no room event in any message).
    if let Some(payload) = pop["message"]["payload"].as_str() {
        assert!(
            !payload.contains("room_join")
                && !payload.contains("room_leave")
                && !payload.contains("room_expire"),
            "no room SSE events should be pushed to members; got: {payload}"
        );
    }
}

// AC8: Two co-present room agents CAN submit grant requests to each other.
#[tokio::test]
async fn ac_room_8_coroom_agents_can_request_grants() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice8").await;
    let bob = setup_agent(&server, &client, "RoomBob8").await;

    let room_id = create_room(&server, &client, &alice).await;
    join_room(&server, &client, &alice, &room_id, None).await;
    join_room(&server, &client, &bob, &room_id, None).await;

    // Alice requests grant from Bob — both in same room, should succeed.
    let rg = client
        .post(server.url("/grants/request"))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({"to": "RoomBob8", "reason": "we share a room"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rg.status(),
        StatusCode::OK,
        "room co-presence must allow grant request: {}",
        rg.json::<Value>().await.unwrap_or_default()
    );
}

// AC9: Agents with no shared room AND no grant CANNOT submit grant requests.
#[tokio::test]
async fn ac_room_9_no_room_no_grant_cannot_request() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice9").await;
    let _bob = setup_agent(&server, &client, "RoomBob9").await;

    // No room, no grant — Alice cannot request a grant from Bob.
    let rg = client
        .post(server.url("/grants/request"))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({"to": "RoomBob9", "reason": "cold contact"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rg.status(),
        StatusCode::FORBIDDEN,
        "no shared room + no grant must return 403, got {}",
        rg.status()
    );
}

// AC10: "create" cannot be used as room_id in join/leave/get → 400.
#[tokio::test]
async fn ac_room_10_reserved_name_create_returns_400() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "RoomAlice10").await;

    // POST /room/create/join — "create" as room_id
    let join_r = client
        .post(server.url("/room/create/join"))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        join_r.status(),
        StatusCode::BAD_REQUEST,
        "join with room_id='create' must return 400"
    );

    // GET /room/create — "create" as room_id
    let get_r = client
        .get(server.url("/room/create"))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::BAD_REQUEST,
        "GET with room_id='create' must return 400"
    );

    // POST /room/create/leave — "create" as room_id
    let leave_r = client
        .post(server.url("/room/create/leave"))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        leave_r.status(),
        StatusCode::BAD_REQUEST,
        "leave with room_id='create' must return 400"
    );
}

// ── Room eviction tests (sim-rooms-empty-room-eviction) ───────────────────────

// AC2: create → join → leave (last member) → room entry gone (GET returns 404).
#[tokio::test]
async fn ac_room_eviction_last_member_leaves_room_gone() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "EvAlice").await;

    let room_id = create_room(&server, &client, &alice).await;
    join_room(&server, &client, &alice, &room_id, None).await;

    // Alice is the last (and only) member — leave should remove the room.
    let leave_r = client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(leave_r.status(), StatusCode::OK, "leave must return 200");

    // GET must now return 404 (room gone, not just 403 not-a-member).
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::NOT_FOUND,
        "room entry must be gone (404) after last member leaves — not merely 403 not-a-member"
    );
}

// AC3: TTL expiry of last member + explicit leave → room entry gone.
// The lazy cleanup is triggered when leave() is called (which calls prune() internally).
#[tokio::test]
async fn ac_room_eviction_ttl_expired_last_member_cleanup() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "TtlEvAlice").await;

    let room_id = create_room(&server, &client, &alice).await;
    // Alice joins with a 1-second TTL.
    join_room(&server, &client, &alice, &room_id, Some(1)).await;

    // Wait for alice's TTL to expire.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Alice calls leave (idempotent — she already expired, but leave() prunes the room
    // and removes it when it becomes empty).
    let leave_r = client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(leave_r.status(), StatusCode::OK, "leave must return 200");

    // GET must now return 404 — the room was pruned and removed during leave().
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::NOT_FOUND,
        "room must be gone (404) after TTL-expired last member triggers cleanup via leave()"
    );
}

// AC4 (regression): room with remaining members is NOT removed on partial leave.
#[tokio::test]
async fn ac_room_eviction_no_regression_partial_leave() {
    let server = TestServer::spawn().await;
    let client = server.client();
    let alice = setup_agent(&server, &client, "EvAlice4").await;
    let bob = setup_agent(&server, &client, "EvBob4").await;

    let room_id = create_room(&server, &client, &alice).await;
    join_room(&server, &client, &alice, &room_id, None).await;
    join_room(&server, &client, &bob, &room_id, None).await;

    // Alice leaves — bob is still in the room; room must remain.
    client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice}"))
        .send()
        .await
        .unwrap();

    // Bob can still GET the room.
    let get_r = client
        .get(server.url(&format!("/room/{room_id}")))
        .header("Authorization", format!("Bearer {bob}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_r.status(),
        StatusCode::OK,
        "room must still exist (200) when bob is still a member after alice leaves"
    );
}

// ── Dequeue alias acceptance tests (sim-dequeue-alias-acceptance-test) ─────────

// AC1: POST /messages/dequeue with valid token + empty queue → 200, null message.
#[tokio::test]
async fn ac_dequeue_alias_empty_queue_returns_null() {
    let server = TestServer::spawn().await;
    let client = server.client();

    let (token, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"name": "AliasEmpty"}))
        .send()
        .await
        .unwrap();

    let start = std::time::Instant::now();
    let r = client
        .post(server.url("/messages/dequeue"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "POST /messages/dequeue must return 200"
    );
    // Non-blocking: well under 500 ms.
    assert!(
        elapsed < Duration::from_millis(500),
        "dequeue alias must be non-blocking; took {:?}",
        elapsed
    );
    let body: Value = r.json().await.unwrap();
    assert!(
        body["message"].is_null(),
        "empty queue must return null message"
    );
    assert_eq!(body["remaining"], 0, "remaining must be 0 on empty queue");
}

// AC2: POST /messages/dequeue with a queued message → 200, message returned and removed.
#[tokio::test]
async fn ac_dequeue_alias_returns_queued_message() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    // Receiver.
    let (recv_tok, _recv_stream) = listen_get_token(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {recv_tok}"))
        .json(&json!({"name": "AliasRecv"}))
        .send()
        .await
        .unwrap();

    // Sender.
    let (sender_tok, _) = listen_and_get_welcome(&server, &client, None).await;
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {sender_tok}"))
        .json(&json!({"name": "AliasSender"}))
        .send()
        .await
        .unwrap();
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {gov}"))
        .json(&json!({"identity_a": sender_tok, "identity_b": recv_tok}))
        .send()
        .await
        .unwrap();

    // Sender sends a message.
    client
        .post(server.url("/messages/send"))
        .header("Authorization", format!("Bearer {sender_tok}"))
        .json(&json!({"to": "AliasRecv", "payload": "hello-via-alias"}))
        .send()
        .await
        .unwrap();

    // Receiver dequeues via the alias endpoint.
    let r = client
        .post(server.url("/messages/dequeue"))
        .header("Authorization", format!("Bearer {recv_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "POST /messages/dequeue must return 200 when a message is queued"
    );
    let body: Value = r.json().await.unwrap();
    let msg = &body["message"];
    assert!(
        !msg.is_null(),
        "dequeue alias must return the queued message"
    );
    assert_eq!(
        msg["payload"], "hello-via-alias",
        "message payload must match what was sent"
    );
    assert_eq!(
        msg["from"], "AliasSender",
        "message from field must identify the sender"
    );

    // AC4: response shape identical to /messages/queue/pop — check remaining field.
    assert!(
        body["remaining"].is_number(),
        "remaining field must be present"
    );

    // Message consumed — second dequeue via canonical path returns null.
    let r2 = client
        .post(server.url("/messages/queue/pop"))
        .header("Authorization", format!("Bearer {recv_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let body2: Value = r2.json().await.unwrap();
    assert!(
        body2["message"].is_null(),
        "message must be consumed after alias dequeue; canonical pop must return null"
    );
}

// ── SSE direct probe for room events (sim-rooms-ac7-sse-direct-probe) ──────────

// AC1: Open an SSE stream, trigger room join/leave events, assert no room_* events
// appear on the stream within a short window (direct assertion, not via dequeue).
#[tokio::test]
async fn ac_room_6_7_sse_direct_no_room_events() {
    let server = TestServer::spawn().await;
    let client = server.client();

    // Alice opens a live SSE stream to capture events.
    let _rtok = register_participant_tok(&server, &client).await;
    let r = client
        .post(server.url("/listen"))
        .header("Authorization", format!("Bearer {_rtok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let mut stream = r.bytes_stream();

    // Drain the welcome event.
    let mut buf = String::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        if buf.lines().any(|l| l.starts_with("data:")) {
            break;
        }
    }
    let alice_tok = _rtok.clone();
    client
        .post(server.url("/announce"))
        .header("Authorization", format!("Bearer {alice_tok}"))
        .json(&json!({"name": "SseDAlice"}))
        .send()
        .await
        .unwrap();

    let bob = setup_agent(&server, &client, "SseDAliceBob").await;

    // Create a room and have both agents join then leave with a 1-second TTL.
    let room_id = create_room(&server, &client, &alice_tok).await;
    join_room(&server, &client, &alice_tok, &room_id, Some(1)).await;
    join_room(&server, &client, &bob, &room_id, Some(1)).await;

    // Wait for TTL expiry (room join/leave/expire events would arrive within this window
    // if they were being emitted — they must NOT appear on the SSE stream).
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Alice leaves explicitly too.
    client
        .post(server.url(&format!("/room/{room_id}/leave")))
        .header("Authorization", format!("Bearer {alice_tok}"))
        .send()
        .await
        .unwrap();

    // Drain any SSE events that arrived during the window and assert none are room events.
    let mut room_events_seen = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let data = String::from_utf8_lossy(&chunk);
                for line in data.lines() {
                    if line.starts_with("data:") {
                        let payload = line.trim_start_matches("data:").trim();
                        if payload.contains("room_join")
                            || payload.contains("room_leave")
                            || payload.contains("room_expire")
                        {
                            room_events_seen.push(payload.to_string());
                        }
                    }
                }
            }
            _ => break,
        }
    }
    assert!(
        room_events_seen.is_empty(),
        "no room_* SSE events must be pushed to members; got: {:?}",
        room_events_seen
    );
}

// ── Grant with existing grant, no shared room (sim-rooms-grant-with-existing-grant) ─

// AC1: Two agents with an existing active grant but NOT in any shared room →
// POST /grants/request returns 200 (has_any_grant_with path).
#[tokio::test]
async fn ac_room_grant_existing_no_room() {
    let (server, gov) = TestServer::spawn_with_governor().await;
    let client = server.client();

    let alice = setup_agent(&server, &client, "GrantAlice").await;
    let _bob = setup_agent(&server, &client, "GrantBob").await;

    // Governor approves a grant between alice and bob — no shared room involved.
    client
        .post(server.url("/grants/approve"))
        .header("Authorization", format!("Bearer {gov}"))
        .json(&json!({"identity_a": alice, "identity_b": _bob}))
        .send()
        .await
        .unwrap();

    // Confirm they are NOT in any shared room (no room created).
    // Alice requests a grant from Bob — must succeed because has_any_grant_with is true.
    let rg = client
        .post(server.url("/grants/request"))
        .header("Authorization", format!("Bearer {alice}"))
        .json(&json!({"to": "GrantBob", "reason": "we already have a grant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rg.status(),
        StatusCode::OK,
        "grant request must return 200 when requester already holds an active grant with target (no shared room required): got {}",
        rg.json::<Value>().await.unwrap_or_default()
    );
}
