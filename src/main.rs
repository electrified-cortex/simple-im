use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use simple_im::http::AppState;
use simple_im::persistence::TokenStore;

struct Config {
    port: u16,
    insecure_http: bool,
    liveness_window_secs: u64,
    token_store_path: PathBuf,
}

impl Config {
    fn from_env_and_args() -> Self {
        let mut port: u16 = 8443;
        let mut insecure_http = false;
        let mut liveness_window_secs: u64 = 30;
        let mut token_store_path: Option<PathBuf> = None;

        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => {
                    println!("Usage: simple-im [OPTIONS]");
                    println!();
                    println!("Options:");
                    println!(
                        "  --insecure-http              Serve plain HTTP (required; no built-in TLS — terminate TLS at a reverse proxy)."
                    );
                    println!(
                        "  --port <PORT>                Port to listen on (default: 8443; 8080 with --insecure-http)"
                    );
                    println!(
                        "  --liveness-window-secs <N>   Liveness window in seconds (default: 30)"
                    );
                    println!(
                        "  --token-store-path <PATH>    SQLite DB for token/grant persistence (default: sim-tokens.db)"
                    );
                    println!("  --help, -h                   Print this help message");
                    std::process::exit(0);
                }
                "--insecure-http" => insecure_http = true,
                "--port" => {
                    i += 1;
                    if let Some(v) = args.get(i) {
                        port = v.parse().unwrap_or(8443);
                    }
                }
                "--liveness-window-secs" => {
                    i += 1;
                    if let Some(v) = args.get(i) {
                        liveness_window_secs = v.parse().unwrap_or(30).clamp(5, 600);
                    }
                }
                "--token-store-path" => {
                    i += 1;
                    if let Some(v) = args.get(i) {
                        token_store_path = Some(PathBuf::from(v));
                    }
                }
                _ => {}
            }
            i += 1;
        }

        if insecure_http {
            port = if port == 8443 { 8080 } else { port };
        }

        if std::env::var("SIMPLE_IM_INSECURE_HTTP").as_deref() == Ok("1") {
            insecure_http = true;
        }
        if let Ok(v) = std::env::var("SIMPLE_IM_LIVENESS_WINDOW_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            liveness_window_secs = n.clamp(5, 600);
        }
        if token_store_path.is_none()
            && let Ok(v) = std::env::var("SIMPLE_IM_TOKEN_STORE")
            && !v.is_empty()
        {
            token_store_path = Some(PathBuf::from(v));
        }

        Config {
            port,
            insecure_http,
            liveness_window_secs,
            token_store_path: token_store_path.unwrap_or_else(|| PathBuf::from("sim-tokens.db")),
        }
    }
}

/// Install a panic hook that emits one greppable line with the panic location +
/// message. The release profile uses `panic = "unwind"`, so a panicking request is
/// isolated to its own tokio task (that request 500s) while the hub keeps serving; this
/// hook ensures we still SEE the panic. It is the primary crash-forensics tool:
/// `docker logs simple-im | grep 'SIM PANIC'` yields `src/<file>.rs:<line>` of the exact
/// panic. Set `RUST_BACKTRACE=1` for a stack trace (symbols are limited under
/// `strip = true`, but frames still help).
fn install_panic_logger() {
    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;
        let loc = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown location>".to_string());
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        let thread = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();
        eprintln!("=== SIM PANIC === thread='{thread}' at {loc}: {msg}");
        eprintln!("backtrace:\n{}", std::backtrace::Backtrace::capture());
        let _ = std::io::stderr().flush();
    }));
}

/// Build identifier (best-effort): `SIM_BUILD` env at compile time, else crate version.
fn build_tag() -> String {
    option_env!("SIM_BUILD")
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("v{}", env!("CARGO_PKG_VERSION")))
}

#[tokio::main]
async fn main() {
    install_panic_logger();
    eprintln!("simple-im starting (build: {})", build_tag());
    let config = Config::from_env_and_args();

    // Open persistent token store and load trust chain.
    let db_path = config.token_store_path.to_string_lossy().into_owned();
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let liveness = Duration::from_secs(config.liveness_window_secs);

    match TokenStore::open(&db_path).await {
        Ok(store) => {
            let store = Arc::new(store);
            let tokens = store.load_tokens().await.unwrap_or_else(|e| {
                eprintln!("WARNING: could not load tokens from store: {e}");
                vec![]
            });
            let grants = store.load_grants().await.unwrap_or_else(|e| {
                eprintln!("WARNING: could not load grants from store: {e}");
                vec![]
            });
            let identities = store.load_identities().await.unwrap_or_else(|e| {
                eprintln!("WARNING: could not load DCP identities from store: {e}");
                vec![]
            });
            let denial_blocks = store.load_denial_blocks().await.unwrap_or_else(|e| {
                eprintln!("WARNING: could not load denial blocks from store: {e}");
                vec![]
            });
            eprintln!(
                "Token store: {} (loaded {} tokens, {} grants, {} DCP identities, {} denial blocks)",
                db_path,
                tokens.len(),
                grants.len(),
                identities.len(),
                denial_blocks.len()
            );
            let hub = simple_im::delivery::DeliveryHub::new_with_persisted_state(
                liveness,
                store,
                tokens,
                grants,
                identities,
                denial_blocks,
            );
            let state = Arc::new(AppState::new_with_hub(hub));
            run(config.insecure_http, addr, state).await;
        }
        Err(e) => {
            eprintln!("WARNING: could not open token store at '{db_path}': {e}");
            eprintln!("WARNING: running without persistence — tokens will be lost on restart.");
            let state = Arc::new(AppState::new(liveness));
            run(config.insecure_http, addr, state).await;
        }
    }
}

async fn run(insecure_http: bool, addr: SocketAddr, state: Arc<AppState>) {
    // Debug (15-DEBUG): periodic in-memory state-size snapshot to stderr every 30s, so
    // unbounded growth (the OOM hypothesis) shows up in `docker logs` as a rising count
    // on a specific collection (e.g. dcp_probes). Spawned before the router takes state.
    {
        let state_for_log = Arc::clone(&state);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tick.tick().await;
                eprintln!("[state] {}", state_for_log.hub.debug_state_sizes());
            }
        });
    }
    let app = simple_im::http::router(state);
    if insecure_http {
        eprintln!("WARNING: running in insecure HTTP mode (--insecure-http). TLS disabled.");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        eprintln!("Listening on http://{addr}");
        axum::serve(listener, app).await.unwrap();
    } else {
        eprintln!(
            "No built-in TLS. Pass --insecure-http and terminate TLS at a reverse proxy (e.g. Caddy/nginx)."
        );
        std::process::exit(1);
    }
}
