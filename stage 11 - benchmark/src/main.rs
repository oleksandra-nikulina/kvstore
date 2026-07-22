mod resp_client;
mod stats;
mod workload;

use resp_client::{read_reply, send_command};
use stats::Stats;
use std::env;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use workload::Workload;

/// Which command(s) the non-pipelined loop sends each iteration.
/// `Both` (the default) is the realistic mixed workload; `GetOnly`/
/// `SetOnly` exist specifically to isolate the store's `RwLock`
/// behavior — `Store`'s reads take a *shared* lock and `GET`s from
/// many clients can run genuinely in parallel, while every `SET` takes
/// the *exclusive* lock, so no two writes, on any keys, ever run at
/// the same time. Splitting the two out is what makes that difference
/// show up as a number instead of a claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandMode {
    Both,
    GetOnly,
    SetOnly,
}

impl std::str::FromStr for CommandMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "both" => Ok(CommandMode::Both),
            "get" => Ok(CommandMode::GetOnly),
            "set" => Ok(CommandMode::SetOnly),
            other => Err(format!("unknown command mode '{other}' (expected both, get, or set)")),
        }
    }
}

impl CommandMode {
    fn name(&self) -> &'static str {
        match self {
            CommandMode::Both => "both",
            CommandMode::GetOnly => "get",
            CommandMode::SetOnly => "set",
        }
    }
}

struct Config {
    port: u16,
    clients: usize,
    warmup: Duration,
    duration: Duration,
    workload: Workload,
    payload_size: usize,
    pipeline: usize,
    command: CommandMode,
    label: String,
    csv: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: bench --port <port> [--clients N] [--warmup-secs N] [--duration-secs N] \
         [--workload set-get|hot-keys|spread-keys] [--command both|get|set] \
         [--payload-size N] [--pipeline N] [--label NAME] [--csv]"
    );
    process::exit(1);
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> String {
    args.next().unwrap_or_else(|| usage())
}

impl Config {
    fn parse() -> Self {
        let mut port: Option<u16> = None;
        let mut clients = 50usize;
        let mut warmup_secs = 1u64;
        let mut duration_secs = 3u64;
        let mut workload = Workload::SetGet;
        let mut payload_size = 64usize;
        let mut pipeline = 1usize;
        let mut command = CommandMode::Both;
        let mut label = String::new();
        let mut csv = false;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => port = Some(next_arg(&mut args).parse().unwrap_or_else(|_| usage())),
                "--clients" => clients = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--warmup-secs" => warmup_secs = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--duration-secs" => duration_secs = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--workload" => workload = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--command" => command = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--payload-size" => payload_size = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--pipeline" => pipeline = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--label" => label = next_arg(&mut args),
                "--csv" => csv = true,
                _ => usage(),
            }
        }

        if clients == 0 || pipeline == 0 {
            usage();
        }

        Config {
            port: port.unwrap_or_else(|| usage()),
            clients,
            warmup: Duration::from_secs(warmup_secs),
            duration: Duration::from_secs(duration_secs),
            workload,
            payload_size,
            pipeline,
            command,
            label,
            csv,
        }
    }
}

/// Primes real data behind whatever keys this client is about to
/// request, before the timed region starts, so pipelined `GET`s (which
/// this tool never alternates with `SET`s — see `run_client`) mostly
/// hit rather than mostly miss. Not load-bearing for the numbers this
/// stage cares about (a hit and a miss cost the same on this project's
/// `HashMap`-backed store), just makes a manual run look like it's
/// doing something real if you're watching. `SpreadKeys` skips this —
/// pre-filling a million keys isn't worth the setup time it'd cost
/// every single run.
async fn prefill(stream: &mut TcpStream, workload: Workload, client_id: usize, payload: &[u8]) {
    match workload {
        Workload::SetGet => {
            let key = format!("bench-client-{client_id}");
            send_command(stream, &[b"SET", key.as_bytes(), payload]).await.unwrap();
            read_reply(stream).await.unwrap();
        }
        Workload::HotKeys => {
            for i in 0..workload::HOT_KEY_COUNT {
                let key = format!("bench-hot-{i}");
                send_command(stream, &[b"SET", key.as_bytes(), payload]).await.unwrap();
                read_reply(stream).await.unwrap();
            }
        }
        Workload::SpreadKeys => {}
    }
}

/// Bundles the per-run knobs `run_client` needs beyond its own identity
/// and the shared barrier — kept as one `Copy` struct instead of five
/// separate parameters purely for signature readability at the call
/// site (clippy's `too_many_arguments` was the immediate prompt, but
/// the struct's a better shape regardless).
#[derive(Clone, Copy)]
struct RunParams {
    workload: Workload,
    payload_size: usize,
    pipeline: usize,
    command: CommandMode,
    warmup: Duration,
    duration: Duration,
}

async fn run_client(id: usize, addr: SocketAddr, params: RunParams, barrier: Arc<Barrier>) -> Vec<Duration> {
    let RunParams { workload, payload_size, pipeline, command, warmup, duration } = params;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).ok();
    let payload = vec![b'x'; payload_size];
    // Distinct, deterministic seed per client so runs are reproducible
    // and no two clients draw the same "random" key sequence.
    let mut rng: u64 = (id as u64).wrapping_mul(2_654_435_761).wrapping_add(1);

    prefill(&mut stream, workload, id, &payload).await;

    // Every client starts its timed loop at the same instant, rather
    // than staggered by however long `tokio::spawn` took to actually
    // schedule each one — otherwise the first few requests of a run
    // would be under lower concurrency than the rest, understating
    // contention right when the warm-up window is supposed to be
    // absorbing exactly that kind of ramp-up noise.
    barrier.wait().await;
    let start = Instant::now();
    let total = warmup + duration;
    let mut samples = Vec::new();

    if pipeline <= 1 {
        loop {
            let elapsed = start.elapsed();
            if elapsed >= total {
                break;
            }
            let key = workload.key_for(id, &mut rng);

            // `Both` records one sample per command (SET and GET are
            // timed and recorded separately, not merged into one
            // "request" latency) specifically so a report can show
            // whether the two behave differently under load — which,
            // for a store using one exclusive lock for all writes and
            // one shared lock for all reads, they should.
            if command != CommandMode::GetOnly {
                let t0 = Instant::now();
                send_command(&mut stream, &[b"SET", key.as_bytes(), &payload]).await.unwrap();
                read_reply(&mut stream).await.unwrap();
                let latency = t0.elapsed();
                if elapsed >= warmup {
                    samples.push(latency);
                }
            }
            if command != CommandMode::SetOnly {
                let t0 = Instant::now();
                send_command(&mut stream, &[b"GET", key.as_bytes()]).await.unwrap();
                read_reply(&mut stream).await.unwrap();
                let latency = t0.elapsed();
                if elapsed >= warmup {
                    samples.push(latency);
                }
            }
        }
    } else {
        // Pipelining: send a whole batch of requests back to back
        // without waiting for each reply, then read all the replies —
        // amortizing one network round trip across `pipeline` requests
        // instead of paying it per request. `GET`-only (not
        // alternating `SET`/`GET`) deliberately: what pipelining
        // demonstrates is round-trip amortization, not this project's
        // command mix, and mixing types here wouldn't change that.
        loop {
            let elapsed = start.elapsed();
            if elapsed >= total {
                break;
            }
            let keys: Vec<String> = (0..pipeline).map(|_| workload.key_for(id, &mut rng)).collect();

            let t0 = Instant::now();
            for key in &keys {
                send_command(&mut stream, &[b"GET", key.as_bytes()]).await.unwrap();
            }
            for _ in 0..pipeline {
                read_reply(&mut stream).await.unwrap();
            }
            let batch_latency = t0.elapsed();

            if elapsed >= warmup {
                let per_request = batch_latency / pipeline as u32;
                samples.extend(std::iter::repeat_n(per_request, pipeline));
            }
        }
    }

    samples
}

fn print_report(config: &Config, stats: &Stats) {
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    if config.csv {
        println!(
            "{},{},{},{},{},{},{},{:.1},{:.3},{:.3},{:.3},{:.3},{:.3}",
            config.label,
            config.clients,
            config.workload.name(),
            config.command.name(),
            config.payload_size,
            config.pipeline,
            stats.count,
            stats.throughput_per_sec(),
            ms(stats.mean),
            ms(stats.p50),
            ms(stats.p95),
            ms(stats.p99),
            ms(stats.max),
        );
    } else {
        println!(
            "label={} port={} clients={} workload={} command={} payload={}B pipeline={}",
            config.label,
            config.port,
            config.clients,
            config.workload.name(),
            config.command.name(),
            config.payload_size,
            config.pipeline
        );
        println!(
            "  requests={} throughput={:.1} req/s",
            stats.count,
            stats.throughput_per_sec()
        );
        println!(
            "  latency(ms): mean={:.3} p50={:.3} p95={:.3} p99={:.3} max={:.3}",
            ms(stats.mean),
            ms(stats.p50),
            ms(stats.p95),
            ms(stats.p99),
            ms(stats.max),
        );
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let addr: SocketAddr = format!("127.0.0.1:{}", config.port)
        .parse()
        .unwrap_or_else(|_| usage());

    let params = RunParams {
        workload: config.workload,
        payload_size: config.payload_size,
        pipeline: config.pipeline,
        command: config.command,
        warmup: config.warmup,
        duration: config.duration,
    };
    let barrier = Arc::new(Barrier::new(config.clients));
    let mut handles = Vec::with_capacity(config.clients);
    for id in 0..config.clients {
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(run_client(id, addr, params, barrier)));
    }

    let mut all_samples = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(samples) => all_samples.extend(samples),
            Err(e) => eprintln!("a client task panicked: {e}"),
        }
    }

    let computed = stats::compute(all_samples, config.duration);
    print_report(&config, &computed);
}
