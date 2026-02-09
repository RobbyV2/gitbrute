use anyhow::{Context, Result, anyhow};
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, bounded};
use regex::bytes::Regex;
use sha1::{Digest, Sha1};
use std::io::{self, Write};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Desired prefix
    #[arg(long, default_value = "")]
    prefix: String,

    /// Desired regex pattern
    #[arg(long, default_value = "")]
    pattern: String,

    /// Re-run, even if current hash matches prefix
    #[arg(long, default_value_t = false)]
    force: bool,

    /// Number of CPUs to use. Defaults to number of processors.
    #[arg(long)]
    cpus: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct Try {
    commit_behind: usize,
    author_behind: usize,
}

#[derive(Debug, Clone)]
struct Solution {
    author: GitDate,
    committer: GitDate,
}

#[derive(Debug, Clone)]
struct GitDate {
    seconds: i64,
    tz: String,
}

struct BruteForceTask {
    blob: Vec<u8>,
    author_date: GitDate,
    committer_date: GitDate,
    author_idx: usize,
    committer_idx: usize,
    prefix: String,
    pattern: Option<Regex>,
    counter: Arc<AtomicU64>,
    rx_try: Receiver<Try>,
    tx_winner: Sender<Solution>,
    rx_done: Receiver<()>,
}

impl std::fmt::Display for GitDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.seconds, self.tz)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.prefix.is_empty() && args.pattern.is_empty() {
        return Err(anyhow!("Need prefix or pattern"));
    }

    // Validate hex prefix
    if !args.prefix.is_empty() && !args.prefix.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("Prefix isn't hex"));
    }

    // Compile pattern if present
    let pattern_rx = if !args.pattern.is_empty() {
        Some(Regex::new(&args.pattern).context("Invalid regex pattern")?)
    } else {
        None
    };

    let hash = cur_hash()?;
    println!("Current hash: {}", hash);

    if matches_requirements(&hash, &args.prefix, pattern_rx.as_ref()) && !args.force {
        println!("Current hash already matches. Use --force to re-run.");
        return Ok(());
    }

    let obj_content = get_object(&hash)?;

    // Find double newline to separate header from body
    let body_start_idx = obj_content
        .windows(2)
        .position(|w| w == b"\n\n")
        .ok_or_else(|| anyhow!("No \\n\\n found in object"))?
        + 2;

    let _msg = &obj_content[body_start_idx..];

    let cpus = args.cpus.unwrap_or_else(num_cpus::get);
    println!("Running with {} CPUs", cpus);

    let author_rx = Regex::new(r"(?m)^author.+> (.+)")?;
    let committer_rx = Regex::new(r"(?m)^committer.+> (.+)")?;

    let header = format!("commit {}\0", obj_content.len());
    let mut blob = Vec::with_capacity(header.len() + obj_content.len());
    blob.extend_from_slice(header.as_bytes());
    blob.extend_from_slice(&obj_content);

    let (author_date, author_idx) = parse_date(&blob, &author_rx)?;
    let (committer_date, committer_idx) = parse_date(&blob, &committer_rx)?;

    println!("Author date: {} (idx: {})", author_date, author_idx);
    println!(
        "Committer date: {} (idx: {})",
        committer_date, committer_idx
    );

    let (tx_try, rx_try) = bounded::<Try>(512);
    let (tx_winner, rx_winner) = bounded::<Solution>(1);
    let (tx_done, rx_done) = bounded::<()>(0);
    let counter = Arc::new(AtomicU64::new(0));

    thread::spawn(move || {
        explore(tx_try);
    });

    let prefix = args.prefix.clone();
    let prefix_len = prefix.len();

    let mut workers = Vec::with_capacity(cpus);
    for _ in 0..cpus {
        let rx_try = rx_try.clone();
        let tx_winner = tx_winner.clone();
        let rx_done = rx_done.clone();
        let blob = blob.clone();
        let author_date = author_date.clone();
        let committer_date = committer_date.clone();
        let prefix = prefix.clone();
        let pattern_rx = pattern_rx.clone();
        let counter = Arc::clone(&counter);

        workers.push(thread::spawn(move || {
            brute_force(BruteForceTask {
                blob,
                author_date,
                committer_date,
                author_idx,
                committer_idx,
                prefix,
                pattern: pattern_rx,
                counter,
                rx_try,
                tx_winner,
                rx_done,
            });
        }));
    }

    drop(tx_winner);

    let display_counter = Arc::clone(&counter);
    let display_done = rx_done.clone();
    thread::spawn(move || {
        display_progress(display_counter, display_done, prefix_len);
    });

    let start = Instant::now();
    let w = rx_winner
        .recv()
        .context("Workers finished without finding a solution")?;

    drop(tx_done);

    let elapsed = start.elapsed();
    let total = counter.load(Ordering::Relaxed);
    print!("\r\x1b[2K");
    println!(
        "Found in {:.2}s | {} attempts | {:.2} Mh/s",
        elapsed.as_secs_f64(),
        format_count(total),
        total as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );
    println!("Solution: Author: {}, Committer: {}", w.author, w.committer);

    write_solution(w, obj_content)?;

    Ok(())
}

fn explore(tx: Sender<Try>) {
    let mut max = 0;
    loop {
        for i in 0..max {
            if tx
                .send(Try {
                    commit_behind: i,
                    author_behind: max,
                })
                .is_err()
            {
                return;
            }
        }
        for j in 0..=max {
            if tx
                .send(Try {
                    commit_behind: max,
                    author_behind: j,
                })
                .is_err()
            {
                return;
            }
        }
        max += 1;
    }
}

fn display_progress(counter: Arc<AtomicU64>, rx_done: Receiver<()>, prefix_len: usize) {
    let start = Instant::now();
    let mut stderr = io::stderr();

    loop {
        match rx_done.recv_timeout(Duration::from_millis(500)) {
            Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
        }

        let total = counter.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            continue;
        }
        let rate = total as f64 / elapsed;

        let progress = search_probability(total, rate, prefix_len);

        let _ = write!(
            stderr,
            "\r\x1b[2K  [{:.1}s] {} attempts | {:.2} Mh/s{}",
            elapsed,
            format_count(total),
            rate / 1_000_000.0,
            progress
        );
        let _ = stderr.flush();
    }
}

fn format_count(n: u64) -> String {
    itertools::Itertools::join(
        &mut n
            .to_string()
            .as_bytes()
            .rchunks(3)
            .rev()
            .map(|chunk| std::str::from_utf8(chunk).unwrap_or("")),
        ",",
    )
}

fn search_probability(attempts: u64, rate: f64, prefix_len: usize) -> String {
    if prefix_len == 0 || rate < 1.0 {
        return " | Chance of finding: N/A".to_string();
    }
    let expected = 16_u64.pow(prefix_len as u32) as f64;
    let probability = 1.0 - (-1.0 * attempts as f64 / expected).exp();
    format!(" | Chance of finding: {:.0}%", probability * 100.0)
}

fn brute_force(mut task: BruteForceTask) {
    let mut hasher = Sha1::new();
    let prefix_bytes = if !task.prefix.is_empty() {
        Some(task.prefix.as_bytes())
    } else {
        None
    };

    loop {
        crossbeam_channel::select! {
            recv(task.rx_done) -> _ => return,
            recv(task.rx_try) -> t => {
                let t = match t {
                    Ok(t) => t,
                    Err(_) => return, // Channel closed
                };

                let ad = GitDate {
                    seconds: task.author_date.seconds - t.author_behind as i64,
                    tz: task.author_date.tz.clone(),
                };
                let cd = GitDate {
                    seconds: task.committer_date.seconds - t.commit_behind as i64,
                    tz: task.committer_date.tz.clone(),
                };

                write_int_at(&mut task.blob, task.author_idx, ad.seconds);
                write_int_at(&mut task.blob, task.committer_idx, cd.seconds);

                hasher.update(&task.blob);
                let result = hasher.finalize_reset();
                let hex_hash = hex::encode(result);
                task.counter.fetch_add(1, Ordering::Relaxed);

                let mut match_found = false;
                if let Some(p) = prefix_bytes {
                    if hex_hash.as_bytes().starts_with(p) {
                        match_found = true;
                    }
                } else if let Some(ref rx) = task.pattern
                    && rx.is_match(hex_hash.as_bytes()) {
                        match_found = true;
                    }

                if match_found {
                    let _ = task.tx_winner.send(Solution { author: ad, committer: cd });
                    return;
                }
            }
        }
    }
}

fn write_int_at(blob: &mut [u8], idx: usize, val: i64) {
    let s = val.to_string();
    let bytes = s.as_bytes();

    if idx + bytes.len() <= blob.len() {
        blob[idx..idx + bytes.len()].copy_from_slice(bytes);
    }
}

fn write_solution(sol: Solution, original_obj: Vec<u8>) -> Result<()> {
    let author_rx = Regex::new(r"(?m)^author (.+>) (.+)")?;
    let committer_rx = Regex::new(r"(?m)^committer (.+>) (.+)")?;

    let mut new_obj = original_obj;

    let new_author_line = format!("author $1 {}", sol.author);
    new_obj = author_rx
        .replace(&new_obj, new_author_line.as_bytes())
        .into_owned();

    let new_committer_line = format!("committer $1 {}", sol.committer);
    new_obj = committer_rx
        .replace(&new_obj, new_committer_line.as_bytes())
        .into_owned();

    let mut child = Command::new("git")
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn git hash-object")?;

    {
        let stdin = child.stdin.as_mut().context("Failed to open stdin")?;
        stdin.write_all(&new_obj)?;
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(anyhow!("git hash-object failed"));
    }

    let new_hash = String::from_utf8(output.stdout)?.trim().to_string();
    println!("New hash: {}", new_hash);

    let update = Command::new("git")
        .args(["update-ref", "HEAD", &new_hash])
        .output()?;

    if !update.status.success() {
        return Err(anyhow!("Failed to update HEAD ref: {:?}", update));
    }

    println!("Successfully updated HEAD to {}", new_hash);

    Ok(())
}

fn cur_hash() -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .context("Failed to run git rev-parse HEAD")?;

    if !output.status.success() {
        return Err(anyhow!("git rev-parse HEAD failed: {:?}", output));
    }

    let s = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(s)
}

fn get_object(hash: &str) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(["cat-file", "-p", hash])
        .output()
        .context("Failed to run git cat-file")?;

    if !output.status.success() {
        return Err(anyhow!("git cat-file failed"));
    }

    Ok(output.stdout)
}

fn matches_requirements(hash: &str, prefix: &str, pattern: Option<&Regex>) -> bool {
    if !prefix.is_empty() {
        return hash.starts_with(prefix);
    }
    if let Some(rx) = pattern {
        return rx.is_match(hash.as_bytes());
    }
    true
}

fn parse_date(blob: &[u8], rx: &Regex) -> Result<(GitDate, usize)> {
    let caps = rx
        .captures(blob)
        .ok_or_else(|| anyhow!("Failed to match regex"))?;
    let m = caps.get(1).unwrap(); // The capture group

    let val = m.as_bytes();
    let val_str = std::str::from_utf8(val)?;

    let space_idx = val_str
        .find(' ')
        .ok_or_else(|| anyhow!("unexpected date format"))?;

    let seconds = val_str[..space_idx].parse::<i64>()?;
    let tz = val_str[space_idx + 1..].to_string();

    Ok((GitDate { seconds, tz }, m.start()))
}
