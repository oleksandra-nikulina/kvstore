use kvstore_stage7::run;
use std::env;
use std::process;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let port: u16 = env::args()
        .nth(1)
        .unwrap_or_else(|| "7878".to_string())
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("usage: kvstore-stage7 [port]");
            process::exit(1);
        });

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        process::exit(1);
    });
    println!("stage 7 KV store (async/tokio) listening on {addr}");

    if let Err(e) = run(listener).await {
        eprintln!("server error: {e}");
        process::exit(1);
    }
}
