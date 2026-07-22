mod resp_client;
mod stats;
mod workload;

use resp_client::{read_reply, send_command};
use stats::Stats;
use std::env;
use std::io;
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
            other => Err(format!(
                "unknown command mode '{other}' (expected both, get, or set)"
            )),
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
                "--warmup-secs" => {
                    warmup_secs = next_arg(&mut args).parse().unwrap_or_else(|_| usage())
                }
                "--duration-secs" => {
                    duration_secs = next_arg(&mut args).parse().unwrap_or_else(|_| usage())
                }
                "--workload" => workload = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--command" => command = next_arg(&mut args).parse().unwrap_or_else(|_| usage()),
                "--payload-size" => {
                    payload_size = next_arg(&mut args).parse().unwrap_or_else(|_| usage())
                }
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
async fn prefill(
    stream: &mut TcpStream,
    workload: Workload,
    client_id: usize,
    payload: &[u8],
) -> io::Result<()> {
    match workload {
        Workload::SetGet => {
            let key = format!("bench-client-{client_id}");
            send_command(stream, &[b"SET", key.as_bytes(), payload]).await?;
            read_reply(stream).await?;
        }
        Workload::HotKeys => {
            for i in 0..workload::HOT_KEY_COUNT {
                let key = format!("bench-hot-{i}");
                send_command(stream, &[b"SET", key.as_bytes(), payload]).await?;
                read_reply(stream).await?;
            }
        }
        Workload::SpreadKeys => {}
    }
    Ok(())
}

/// Sends one command and times the full round trip to its reply.
async fn timed_command(stream: &mut TcpStream, parts: &[&[u8]]) -> io::Result<Duration> {
    let t0 = Instant::now();
    send_command(stream, parts).await?;
    read_reply(stream).await?;
    Ok(t0.elapsed())
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

/// `true` in the first slot means this client ran its full measurement
/// window without a fatal I/O error; `false` means it bailed out early
/// (connect/prefill failure, or a send/read error mid-run) and
/// `main`'s report should treat this as *effective concurrency lower
/// than `--clients` claims*, not silently fold in whatever partial
/// samples it managed to collect as if nothing happened. Found by
/// review: the previous version `.unwrap()`'d every I/O call, so a
/// single dropped connection under load either panicked one client
/// (silently reducing concurrency for the rest of a run, with the
/// final report showing only a lower throughput number and no
/// indication why) or, worse, panicked a client *before* it reached
/// the barrier below, permanently hanging every other client's
/// `barrier.wait()` — a barrier can only release once *every*
/// registered participant has called `wait()`, and a client that
/// panics before doing so can never satisfy that count.
async fn run_client(
    id: usize,
    addr: SocketAddr,
    params: RunParams,
    barrier: Arc<Barrier>,
) -> (bool, Vec<Duration>) {
    let RunParams {
        workload,
        payload_size,
        pipeline,
        command,
        warmup,
        duration,
    } = params;

    let mut stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("client {id}: connect failed: {e}");
            // Still register at the barrier so this failure can't
            // strand every other client waiting on it forever.
            barrier.wait().await;
            return (false, Vec::new());
        }
    };
    stream.set_nodelay(true).ok();
    let payload = vec![b'x'; payload_size];
    // Distinct, deterministic seed per client so runs are reproducible
    // and no two clients draw the same "random" key sequence.
    let mut rng: u64 = (id as u64).wrapping_mul(2_654_435_761).wrapping_add(1);

    if let Err(e) = prefill(&mut stream, workload, id, &payload).await {
        eprintln!("client {id}: prefill failed: {e}");
        barrier.wait().await;
        return (false, Vec::new());
    }

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
    let mut succeeded = true;

    if pipeline <= 1 {
        'outer: loop {
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
                match timed_command(&mut stream, &[b"SET", key.as_bytes(), &payload]).await {
                    Ok(latency) => {
                        if elapsed >= warmup {
                            samples.push(latency);
                        }
                    }
                    Err(e) => {
                        eprintln!("client {id}: SET failed mid-run: {e}");
                        succeeded = false;
                        break 'outer;
                    }
                }
            }
            if command != CommandMode::SetOnly {
                match timed_command(&mut stream, &[b"GET", key.as_bytes()]).await {
                    Ok(latency) => {
                        if elapsed >= warmup {
                            samples.push(latency);
                        }
                    }
                    Err(e) => {
                        eprintln!("client {id}: GET failed mid-run: {e}");
                        succeeded = false;
                        break 'outer;
                    }
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
        //
        // What gets recorded per reply is time-since-batch-dispatch,
        // not an isolated per-request round trip — pipelining doesn't
        // have an "isolated round trip" to measure, by construction:
        // requests share one trip. This is deliberately *not* a flat
        // `batch_total / pipeline` average either (an earlier version
        // did that, found in review to be misleading — it flattens
        // every reply in a batch to the identical value, discarding the
        // real, expected trend of later replies in a batch taking
        // longer than earlier ones). Recording actual arrival time
        // keeps that trend visible; just don't read the resulting
        // percentiles as "the latency of one isolated request," they're
        // "how long into its batch this reply arrived."
        'outer: loop {
            let elapsed = start.elapsed();
            if elapsed >= total {
                break;
            }
            let keys: Vec<String> = (0..pipeline)
                .map(|_| workload.key_for(id, &mut rng))
                .collect();

            let batch_start = Instant::now();
            let mut send_error = None;
            for key in &keys {
                if let Err(e) = send_command(&mut stream, &[b"GET", key.as_bytes()]).await {
                    send_error = Some(e);
                    break;
                }
            }
            if let Some(e) = send_error {
                eprintln!("client {id}: pipelined send failed mid-run: {e}");
                succeeded = false;
                break 'outer;
            }

            for _ in 0..pipeline {
                match read_reply(&mut stream).await {
                    Ok(()) => {
                        let latency = batch_start.elapsed();
                        if elapsed >= warmup {
                            samples.push(latency);
                        }
                    }
                    Err(e) => {
                        eprintln!("client {id}: pipelined read failed mid-run: {e}");
                        succeeded = false;
                        break 'outer;
                    }
                }
            }
        }
    }

    (succeeded, samples)
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
    let mut failed_clients = 0usize;
    for handle in handles {
        match handle.await {
            Ok((succeeded, samples)) => {
                all_samples.extend(samples);
                if !succeeded {
                    failed_clients += 1;
                }
            }
            Err(e) => {
                eprintln!(
                    "a client task panicked (unexpected, not a connection/protocol failure): {e}"
                );
                failed_clients += 1;
            }
        }
    }

    // A dropped client shows up as a lower `count`/throughput number
    // with nothing else to distinguish "the server got slower" from
    // "the load generator lost a client" — found by review as a real
    // way to misread a result. Surfacing it here, loudly, on every run
    // (not just in `--csv` mode where it'd be easy to miss in a
    // spreadsheet) is the fix.
    if failed_clients > 0 {
        eprintln!(
            "WARNING: {failed_clients}/{} clients failed to complete their full run (see errors above) \
             — effective concurrency was lower than --clients for at least part of this run, \
             throughput/latency below should not be trusted at face value.",
            config.clients
        );
    }

    let computed = stats::compute(all_samples, config.duration);
    print_report(&config, &computed);
}
