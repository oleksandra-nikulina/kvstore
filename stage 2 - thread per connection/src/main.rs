use kvstore_stage2::run;
use std::env;
use std::net::TcpListener;
use std::process;

fn main() {
    let port: u16 = env::args()
        .nth(1)
        .unwrap_or_else(|| "7878".to_string())
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("usage: kvstore-stage2 [port]");
            process::exit(1);
        });

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        process::exit(1);
    });
    println!("stage 2 echo server (thread-per-connection) listening on {addr}");

    if let Err(e) = run(listener) {
        eprintln!("server error: {e}");
        process::exit(1);
    }
}
