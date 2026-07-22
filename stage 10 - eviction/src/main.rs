use kvstore_stage10::eviction::Policy;
use kvstore_stage10::persistence::{Aof, replay};
use kvstore_stage10::pubsub::PubSub;
use kvstore_stage10::run;
use kvstore_stage10::store::Store;
use std::env;
use std::path::Path;
use std::process;
use std::sync::Arc;
use tokio::net::TcpListener;

fn usage() -> ! {
    eprintln!("usage: kvstore-stage10 [port] [aof_path] [--maxmemory <bytes>] [--policy lru|lfu]");
    process::exit(1);
}

#[tokio::main]
async fn main() {
    let mut maxmemory: Option<usize> = None;
    let mut policy = Policy::Lru;
    let mut positional = Vec::new();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--maxmemory" => {
                let value = args.next().unwrap_or_else(|| usage());
                maxmemory = Some(value.parse().unwrap_or_else(|_| usage()));
            }
            "--policy" => {
                let value = args.next().unwrap_or_else(|| usage());
                policy = match value.as_str() {
                    "lru" => Policy::Lru,
                    "lfu" => Policy::Lfu,
                    _ => usage(),
                };
            }
            other => positional.push(other.to_string()),
        }
    }

    let port: u16 = match positional.first() {
        Some(p) => p.parse().unwrap_or_else(|_| usage()),
        None => 7878,
    };
    let aof_path = positional
        .get(1)
        .cloned()
        .unwrap_or_else(|| "kvstore.aof".to_string());

    let store = Arc::new(match maxmemory {
        Some(bytes) => Store::with_eviction(bytes, policy),
        None => Store::new(),
    });

    let replayed = replay(Path::new(&aof_path), &store)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to replay AOF {aof_path}: {e}");
            process::exit(1);
        });
    println!("AOF: replayed {replayed} command(s) from {aof_path}");

    let aof = Arc::new(Aof::open(Path::new(&aof_path)).await.unwrap_or_else(|e| {
        eprintln!("failed to open AOF {aof_path}: {e}");
        process::exit(1);
    }));
    let pubsub = Arc::new(PubSub::new());

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        process::exit(1);
    });
    match maxmemory {
        Some(bytes) => println!(
            "stage 10 KV store listening on {addr}, logging to {aof_path}, maxmemory={bytes} bytes, policy={policy:?}"
        ),
        None => println!(
            "stage 10 KV store listening on {addr}, logging to {aof_path}, no memory limit"
        ),
    }

    if let Err(e) = run(listener, store, aof, pubsub).await {
        eprintln!("server error: {e}");
        process::exit(1);
    }
}
