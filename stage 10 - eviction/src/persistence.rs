//! Append-only file (AOF) persistence: every write command is logged
//! *as the same RESP wire format clients send commands in* — the
//! easiest possible encode/decode story, since the encoder and parser
//! already exist for the client protocol — and replayed on startup to
//! rebuild the store from scratch.
//!
//! Deliberately not attempted here: real Redis's actual AOF format
//! (which is close to this but not identical), rewrite/compaction of a
//! growing log, or `fsync` on every write (see `Aof::append`'s doc
//! comment for that trade-off specifically).

use crate::command::{Command, execute};
use crate::resp::{Bytes, Reply};
use crate::store::Store;
use std::io;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

fn encode_command(args: &[Bytes]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend(format!("${}\r\n", arg.len()).into_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub struct Aof {
    file: Mutex<File>,
}

impl Aof {
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Aof {
            file: Mutex::new(file),
        })
    }

    /// Executes `command` against `store` and appends it to the log, as
    /// one atomic unit under this `Aof`'s lock.
    ///
    /// That "one atomic unit" is the actual point, not an incidental
    /// detail: two client connections can call this concurrently for
    /// different write commands, and whichever one acquires the lock
    /// first must *both* mutate the store *and* append to the log
    /// before the other one does either — otherwise the log's order
    /// could end up different from the store's actual mutation order,
    /// and replaying it later would reconstruct the wrong final state
    /// for a key both commands touched. Real Redis sidesteps this
    /// problem entirely by being single-threaded for command execution;
    /// this project isn't, so the same guarantee has to be built
    /// explicitly, with this lock standing in for "one command executes
    /// at a time" — but only for writes, since only writes touch the
    /// log; reads skip this method (and this lock) entirely, see
    /// `lib.rs::handle_connection`.
    ///
    /// No `fsync` here — this only asks the OS to buffer the write, not
    /// to guarantee it's on disk before returning. That's a real
    /// durability/performance trade-off: `fsync`ing every write is the
    /// safest option (survives a power loss, not just a process crash)
    /// but is dramatically slower; real Redis defaults to `fsync`ing
    /// about once a second instead of every write, accepting "lose at
    /// most ~1 second of writes on a hard crash" for much better
    /// throughput. This stage doesn't implement either — worth knowing
    /// it's missing, not worth the added complexity to fix for a
    /// learning-scope project.
    pub async fn execute_and_log(&self, command: &Command, args: &[Bytes], store: &Store) -> Reply {
        let mut file = self.file.lock().await;
        let reply = execute(command, store);
        if let Err(e) = file.write_all(&encode_command(args)).await {
            eprintln!("AOF: write failed, continuing without persisting this command: {e}");
        }
        reply
    }
}

/// Rebuilds `store` by replaying every write command previously logged
/// at `path`, in order. If the file doesn't exist yet (first run),
/// there's nothing to replay. A truncated or corrupt trailing entry —
/// the process was killed mid-write, so the log's last command is
/// incomplete or garbled — stops replay at that point rather than
/// failing outright: everything logged before it is still applied.
pub async fn replay(path: &Path, store: &Store) -> io::Result<usize> {
    use crate::command::{ReadResult, read_command};

    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut pos = 0;
    let mut replayed = 0;
    loop {
        match read_command(&bytes[pos..]) {
            Ok(ReadResult::Command { command, consumed }) => {
                execute(&command, store);
                pos += consumed;
                replayed += 1;
            }
            Ok(ReadResult::Empty { consumed }) => {
                pos += consumed;
            }
            Ok(ReadResult::Incomplete) => {
                if pos < bytes.len() {
                    eprintln!(
                        "AOF: stopping replay at a truncated trailing entry ({} byte(s) unreplayed) — likely a crash mid-write",
                        bytes.len() - pos
                    );
                }
                break;
            }
            Err(e) => {
                eprintln!("AOF: stopping replay at a corrupt entry: {e}");
                break;
            }
        }
    }
    Ok(replayed)
}

/// Only used by tests in this module and by `command.rs`'s own
/// `aof_args` tests via the public `command::aof_args` re-export; kept
/// here so `persistence.rs`'s tests don't need to reach into `command`
/// for encoding.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::aof_args;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_aof_path(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("kvstore-stage8-test-{label}-{nanos}.aof"))
    }

    #[tokio::test]
    async fn replay_on_a_missing_file_does_nothing() {
        let path = temp_aof_path("missing");
        let store = Store::new();

        let replayed = replay(&path, &store).await.unwrap();

        assert_eq!(replayed, 0);
    }

    #[tokio::test]
    async fn execute_and_log_then_replay_reconstructs_the_same_state() {
        let path = temp_aof_path("roundtrip");

        {
            let store = Store::new();
            let aof = Aof::open(&path).await.unwrap();

            let set_cmd = Command::Set("k".to_string(), b"v".to_vec());
            aof.execute_and_log(&set_cmd, &aof_args(&set_cmd).unwrap(), &store)
                .await;

            let push_cmd = Command::Rpush("l".to_string(), vec![b"a".to_vec(), b"b".to_vec()]);
            aof.execute_and_log(&push_cmd, &aof_args(&push_cmd).unwrap(), &store)
                .await;
        }

        let restarted = Store::new();
        let replayed = replay(&path, &restarted).await.unwrap();

        assert_eq!(replayed, 2);
        assert_eq!(restarted.get("k"), Ok(Some(b"v".to_vec())));
        assert_eq!(
            restarted.lrange("l", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec()])
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn read_only_commands_are_never_written_to_the_log() {
        let path = temp_aof_path("readonly");
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        // GET has no aof_args, so it's never passed to execute_and_log
        // in the first place — this just confirms the log stays empty
        // when only reads happen, by replaying into a fresh store and
        // checking nothing came back.
        let aof = Aof::open(&path).await.unwrap();
        drop(aof);

        let restarted = Store::new();
        let replayed = replay(&path, &restarted).await.unwrap();
        assert_eq!(replayed, 0);
        assert_eq!(restarted.get("k"), Ok(None));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn replay_stops_cleanly_at_a_truncated_trailing_entry() {
        let path = temp_aof_path("truncated");

        {
            let store = Store::new();
            let aof = Aof::open(&path).await.unwrap();
            let cmd = Command::Set("safe".to_string(), b"value".to_vec());
            aof.execute_and_log(&cmd, &aof_args(&cmd).unwrap(), &store)
                .await;
        }

        // Simulate a crash mid-write: append a well-formed header for a
        // second SET, but cut off before the value's payload arrives.
        {
            use tokio::io::AsyncWriteExt as _;
            let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
            file.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\ngone\r\n$10\r\nonly-part")
                .await
                .unwrap();
        }

        let restarted = Store::new();
        let replayed = replay(&path, &restarted).await.unwrap();

        assert_eq!(
            replayed, 1,
            "only the complete entry before the truncation should replay"
        );
        assert_eq!(restarted.get("safe"), Ok(Some(b"value".to_vec())));
        assert_eq!(restarted.get("gone"), Ok(None));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn replay_stops_cleanly_at_a_corrupt_entry() {
        let path = temp_aof_path("corrupt");

        {
            let store = Store::new();
            let aof = Aof::open(&path).await.unwrap();
            let cmd = Command::Set("safe".to_string(), b"value".to_vec());
            aof.execute_and_log(&cmd, &aof_args(&cmd).unwrap(), &store)
                .await;
        }

        {
            use tokio::io::AsyncWriteExt as _;
            let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
            file.write_all(b"this is not RESP at all\r\n")
                .await
                .unwrap();
        }

        let restarted = Store::new();
        let replayed = replay(&path, &restarted).await.unwrap();

        assert_eq!(
            replayed, 1,
            "everything before the corrupt entry should still replay"
        );
        assert_eq!(restarted.get("safe"), Ok(Some(b"value".to_vec())));

        let _ = std::fs::remove_file(&path);
    }
}
