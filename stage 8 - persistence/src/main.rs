use kvstore_stage8::persistence::{Aof, replay};
use kvstore_stage8::run;
use kvstore_stage8::store::Store;
use std::env;
use std::path::Path;
use std::process;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let _bin = args.next();
    let port: u16 = args
        .next()
        .unwrap_or_else(|| "7878".to_string())
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("usage: kvstore-stage8 [port] [aof_path]");
            process::exit(1);
        });
    let aof_path = args.next().unwrap_or_else(|| "kvstore.aof".to_string());

    let store = Arc::new(Store::new());
    let replayed = replay(Path::new(&aof_path), &store).await.unwrap_or_else(|e| {
        eprintln!("failed to replay AOF {aof_path}: {e}");
        process::exit(1);
    });
    println!("AOF: replayed {replayed} command(s) from {aof_path}");

    let aof = Arc::new(Aof::open(Path::new(&aof_path)).await.unwrap_or_else(|e| {
        eprintln!("failed to open AOF {aof_path}: {e}");
        process::exit(1);
    }));

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        process::exit(1);
    });
    println!("stage 8 KV store (with AOF persistence) listening on {addr}, logging to {aof_path}");

    if let Err(e) = run(listener, store, aof).await {
        eprintln!("server error: {e}");
        process::exit(1);
    }
}
