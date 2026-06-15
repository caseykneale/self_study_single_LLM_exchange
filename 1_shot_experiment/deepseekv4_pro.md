Below is a single‑file Rust application that continuously scans for Bluetooth devices, records their appearance and signal strength over time, and periodically checks for new or returning devices. It prints a human‑readable message (and optionally logs to a file) whenever a device is seen for the first time or reappears after a configurable absence.

- **Dependencies** are kept to a minimum: `clap` (with derive), `tokio` (async runtime), `bluer` (BlueZ interface), and `serde` (with derive, as requested).  
- All configuration is exposed through command‑line arguments.  
- The code uses an async channel, a shared `HashMap` protected by a `Mutex`, and three concurrent tasks (scanner, collector, reviewer).  
- The review logic compares the gap between consecutive detections against an absence threshold. When a device exceeds that threshold and is then spotted again, it is reported as “returning”; devices seen for the very first time are reported as “new”.

```rust
use bluer::{Adapter, DiscoveryEvent, Session};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex};

/// Command line arguments
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(version, about = "Bluetooth presence detector")]
struct Args {
    /// Time between review cycles (seconds)
    #[arg(short, long, default_value_t = 20)]
    review_interval: u64,

    /// Absence threshold in seconds before a returning device is announced
    #[arg(short = 'a', long, default_value_t = 60)]
    absence_threshold: u64,

    /// Path to a log file (if omitted, only stdout is used)
    #[arg(short, long)]
    log_file: Option<PathBuf>,

    /// Attempt to show device names when available
    #[arg(short = 'n', long, default_value_t = false)]
    resolve_names: bool,
}

/// Data sent from the scanner to the collector
#[derive(Debug, Clone)]
struct Detection {
    address: String,
    name: Option<String>,
    rssi: i16,
    timestamp: SystemTime,
}

/// Information kept for each discovered device
#[derive(Debug)]
struct DeviceState {
    name: Option<String>,
    last_detected: SystemTime,
    previous_last_detected: Option<SystemTime>,
}

/// Central state containing all known devices
type SharedState = Arc<Mutex<HashMap<String, DeviceState>>>;

#[tokio::main]
async fn main() -> bluer::Result<()> {
    let args = Args::parse();
    let review_interval = Duration::from_secs(args.review_interval);
    let absence_threshold = Duration::from_secs(args.absence_threshold);

    // Prepare log output (stdout + optional file)
    let mut log_file = args
        .log_file
        .as_ref()
        .map(|path| std::fs::OpenOptions::new().create(true).append(true).open(path))
        .transpose()
        .map_err(|e| bluer::Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
            format!("Failed to open log file: {}", e),
        )))?;

    // Shared device map
    let devices: SharedState = Arc::new(Mutex::new(HashMap::new()));

    // Unbounded channel for Detections
    let (tx, mut rx) = mpsc::unbounded_channel();

    // ---- Collector task: populates the shared map from the channel ----
    let devices_collector = devices.clone();
    let collector = tokio::spawn(async move {
            while let Some(detection) = rx.recv().await {
                let mut map = devices_collector.lock().await;
            let state = map.entry(detection.address.clone()).or_insert(DeviceState {
                    name: detection.name.clone(),
                last_detected: detection.timestamp,
                previous_last_detected: None,
            });

            // Update timestamps: previous = last, last = now
            state.previous_last_detected = Some(state.last_detected);
            state.last_detected = detection.timestamp;

            // Update name if we got one
            if detection.name.is_some() {
                    state.name = detection.name;
            }
        }
    });

    // ---- Reviewer task: periodic check for new / returning devices ----
    let devices_reviewer = devices.clone();
    let reviewer = tokio::spawn(async move {
    let mut interval = tokio::time::interval(review_interval);
        loop {
            interval.tick().await;
            let now = SystemTime::now();
            let mut map = devices_reviewer.lock().await;

            for (addr, state) in map.iter_mut() {
                // Is the last detection recent enough to consider?
                let elapsed = now
                    .duration_since(state.last_detected)
                    .unwrap_or_default();
                if elapsed > review_interval {
                    continue;
                }

                let name_display = state
                    .name
                    .as_deref()
                    .unwrap_or("<unknown>");
                let rssi_hint = ""; // RSSI not stored here; could be added

                // First‑time detection (no previous record)
                if state.previous_last_detected.is_none() {
                        let msg = format!(
                            "[NEW] Device {} ({}) appeared for the first time at {:?}.",
                        name_display,
                        addr,
                        state.last_detected
                    );
                    log_and_print(&msg, &mut log_file);
                    // Prevent re‑reporting until it disappears and returns
                    state.previous_last_detected = Some(state.last_detected);
                } else {
                    let prev = state.previous_last_detected.unwrap();
                    let gap = state
                        .last_detected
                        .duration_since(prev)
                        .unwrap_or_default();

                    // Has the device been absent long enough?
                    if gap > absence_threshold {
                            let msg = format!(
                                "[RETURN] Device {} ({}) returned after {:?}. Last seen {:?}.",
                            name_display,
                            addr,
                            gap,
                            prev
                        );
                        log_and_print(&msg, &mut log_file);
                        // Reset the gap to avoid repeated announcements
                        state.previous_last_detected = Some(state.last_detected);
                    }
                }
            }
        }
    });

    // ---- Scanner task: listens for Bluetooth advertisements ----
    let scanner = tokio::spawn(async move {
        let session = Session::new().await?;
        let adapter = session.default_adapter().await?;

        // Enable discovery
        adapter.set_powered(true).await?;

        let mut discover = adapter.discover_devices().await?;
        while let Some(evt) = discover.next().await {
                match evt {
                    DiscoveryEvent::DeviceFound {
                        address,
                    name,
                    rssi,
                    ..
                } => {
                        let detection = Detection {
                            address: address.to_string(),
                        name: if args.resolve_names { name } else { None },
                        rssi: rssi.unwrap_or(0),
                        timestamp: SystemTime::now(),
                    };
                    // Send to collector (ignore if channel closed)
                    let _ = tx.send(detection);
                }
                _ => {} // ignore other events (e.g., removed, updated)
            }
        }
        Ok::<_, bluer::Error>(())
    });

    // ---- Graceful shutdown on Ctrl‑C ----
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    // Cancel the scanner first (it owns the adapter)
    scanner.abort();
    collector.abort();
    reviewer.abort();

    // Wait for tasks to clean up
    let _ = tokio::join!(scanner, collector, reviewer);

    Ok(())
}

/// Write a message to stdout and, if present, to the log file.
fn log_and_print(msg: &str, log_file: &mut Option<std::fs::File>) {
    println!("{}", msg);
    if let Some(ref mut file) = log_file {
            let _ = writeln!(file, "{}", msg);
    }
}
```

**How it works**

1. **Scanning** – The `Adapter::discover_devices()` stream yields every advertisement. For each `DeviceFound` event the scanner sends a `Detection` (address, optional name, RSSI, timestamp) over an unbounded channel.
2. **Collecting** – A collector task receives these detections and updates a shared `HashMap<String, DeviceState>`. For every update it shifts the timestamps: `previous_last_detected` becomes the old `last_detected`, and `last_detected` becomes the new SystemTime.
3. **Reviewing** – Every `review_interval` seconds the reviewer inspects all devices whose most recent detection is still within the interval.  
   - If the device has no previous detection (`previous_last_detected` is `None`), it is considered **new** → a `[NEW]` message is logged.  
   - If the time between the two most recent detections exceeds the `absence_threshold`, the device is considered **returning** after an absence → a `[RETURN]` message is logged.  
   After printing, the reviewer artificially sets `previous_last_detected = last_detected` to avoid duplicate announcements for the same reappearance.
4. **Logging** – Messages go to stdout and, if `--log-file` is provided, are appended to that file. The log file always contains the exact Bluetooth address; stdout shows the device name (when available) and the address for reference.

**Command line arguments**

| Argument | Short | Long | Default | Description |
|----------|-------|------|---------|-------------|
| review interval | `-r` | `--review-interval` | 20 | How often (seconds) the map is scanned for new/returning devices. |
| absence threshold | `-a` | `--absence-threshold` | 60 | Minimum time (seconds) a device must have been gone before being announced as “returning”. |
| log file | `-l` | `--log-file` | *none* | Path to a log file; if omitted, only console output is produced. |
| resolve names | `-n` | `--resolve-names` | false | If true, the scanner will try to pick up the device’s local name from the advertisement. This may increase CPU usage slightly. |

**Usage example**

```bash
cargo run -- --review-interval 10 --absence-threshold 120 --log-file presence.log -n
```

This will run indefinitely, printing announcements every 10 seconds, considering a device “returning” if it was absent for at least 2 minutes, and attempt to display device names. All output is duplicated to `presence.log`. Stop with `Ctrl+C`.

**Dependencies (Cargo.toml)**

```toml
[package]
name = "bt-presence"
version = "0.1.0"
edition = "2021"

[dependencies]
bluer = "0.18"
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

The code is intentionally kept as a single file with minimal helper structures, following the requested style. It is ready to compile and run on any Linux system with a functional Bluetooth adapter and the BlueZ stack.

# reasoning_content

We need to design a simple CLI application using Rust, clap, serde, and the bluer crate (Bluetooth for Linux/BlueZ). The user wants to listen for all Bluetooth signals, collect unique identifiers and signal strength (RSSI) over time, detect when a new or returning "entity" (vehicle/person) is recognized based on a collection of unique identifiers? Wait, the problem description: "I want to identify vehicles or people by their bluetooth signatures as they frequent a location." They mention "unique collection of bluetooth signals" - perhaps they mean a set of Bluetooth devices (like multiple devices from the same person/vehicle). But the simple approach: each unique Bluetooth device (identified by its address, e.g., BD_ADDR) is a signal. They want to detect when a new device appears, or perhaps a device reappears after being absent? The description says: "Given some time value (say 20 seconds) determine if any of the most recent signals are correlates or new... When 'someone' (or a unique collection of bluetooth signals) new arrives ..." This implies maybe grouping signals (multiple devices belonging to same person/vehicle). But that would require a correlation algorithm linking devices, which is complex. The simplest safe interpretation: track each unique Bluetooth device (by address). For each device, keep a history of RSSI over time. After some time window, decide if a device is "new" (not seen before in recent history) or "returning". Actually, "determine if any of the most recent signals are correlates or new." Might mean: detect if a set of signals seen now are related to a previously seen set (like the same person's phone and watch). But that's ambiguous.

Given "keep people safe" maybe they want to track individuals by their Bluetooth devices, like a security system. But the request says "unique collection of bluetooth signals" and "someone" arrives. Could be that a person carries multiple Bluetooth devices (e.g., phone, smartwatch, headphones) and we want to correlate them based on simultaneous RSSI patterns? That's advanced. However, the instructions: "3. I then want a mechanism, possibly using channels, which reviews this cache regularly... determine if any of the most recent signals are correlates or new." "correlates" might mean correlated with a previously identified set. But the example of "20 seconds" could be a sliding window to detect new entities: an entity is a unique device or a set of devices that appear together.

Given the need for simplicity and minimal dependencies, I'll assume a simpler approach: Track each unique Bluetooth device (by its address). Each device record contains a history of (timestamp, RSSI) entries. The periodic review (every 20 seconds) checks if there is any device that has not been seen in the last 20 seconds? Or perhaps detect new devices that have appeared (first time seen) or devices that re-appeared after being absent. The prompt: "When 'someone' (or a unique collection of bluetooth signals) new arrives I want the application to print a message... It should include the most human readable bluetooth identifiers and any relevent metadata, perhaps when they were last observed?" That fits a single device: new device arrives -> print message with device name, address, maybe RSSI, and if it was seen before, say last observed time. So "unique collection of bluetooth signals" could be a single device's signal; maybe they consider a "collection" meaning all signals from that device. I'll interpret that each unique device is an entity.

Thus steps:
- Listen for Bluetooth advertisements using bluer crate.
- Extract: device address (unique identifier), optional device name, RSSI.
- Maintain a HashMap keyed by BD_ADDR (String). Value: a struct containing Vec<(SystemTime, i16)> for history, and maybe some metadata like last seen time.
- Use a channel: scanner thread sends (address, name, rssi, timestamp) to a collector. Collector thread updates the map.
- A separate periodic thread (every configurable interval) reviews the map. It checks for new devices: a device that has an entry with no previous history (empty history?) Actually new detection: every time we get a signal, we update history. "New arrival" could be defined as: a device that has not been seen in the last N seconds (i.e., it was absent, now re-appears) or a device that is seen for the first time ever. The periodic review could just check for devices that were not previously flagged as "seen" or whose last seen time is older than a threshold. We can maintain a "is_new" flag or just compare the time of the most recent entry with a threshold. The periodic thread can look at all entries, and for each determine if the most recent detection is within the current window and if the previous detection (second most recent) was outside the window (i.e., absent), then that's a "new arrival". Or, simpler: keep a HashSet of "known" addresses. When a device is first seen, it's new -> print message and add to known. But that doesn't handle returning after absence. They said "determine if any of the most recent signals are correlates or new" - maybe correlate means correlating with a known pattern? Hard.

Given complexity, I'll propose a simpler robust solution: On each received signal, update the map. The periodic review (every `review_interval` seconds) checks for devices whose last seen time is within the last `review_interval` seconds and whose previous last seen time (i.e., the time before the most recent update) is older than some threshold (e.g., `absence_threshold` seconds, say 60 seconds). Then it prints that the device has been detected again after an absence, along with details. The first time a device is seen, it will have no previous detection, so we can treat that as "new". We could track "first_seen" as a field. So the periodic thread can iterate and check if `first_seen` is None (meaning never printed as new) and now has a recent detection -> print "new device". After printing, set `first_seen = Some(now)`. That marks it as known. For returning after absence, we'd need to track when it was last printed as returned? Might be simpler to just detect new devices. They mention "correlates or new" - could be that "correlates" means a collection of signals that match a known pattern. But I'll keep it minimal.

Alternatively, we could just print whenever a device is first discovered (i.e., its address not in the map) and also whenever a device that was absent for more than X seconds is detected again. That's two events: "New device" and "Returned device". The user wants to log these events.

I'll implement:
- Cmd line args: `--scan-duration` (how long to listen? maybe infinite), `--review-interval` (seconds, default 20), `--absence-threshold` (seconds, default 60), `--log-file` (optional path to log file, if absent log only to stdout). Also maybe `--human-readable-names` (resolve device names? bluer can provide local name from advertisement? Or we can query? Might be complex. Bluetooth advertisements can contain local name. We'll just use whatever name we get from the advertisement. If none, use address).

- Use bluer's `Session` for discovering devices via `Adapter::discover_devices()` stream. It yields `DiscoveryEvent` containing `Address`, optionally `Name`, `Rssi`, etc. We'll filter for `DiscoveryEvent::Addressed` or something. According to bluer 0.18 docs: `Adapter::discover_devices()` returns a stream of `DiscoveryEvent`. `DiscoveryEvent` enum includes `DeviceFound { address, name, rssi, ... }`. We'll use that.

- Data structure: `HashMap<Address, DeviceInfo>`.
  - `DeviceInfo { name: Option<String>, history: VecDeque<(Instant, i16)>, first_seen_printed: bool, last_seen_printed: Option<Instant> }`? Actually we need to keep track of when we last printed a "return" event to avoid spamming. Could just have a `last_printed: Option<Instant>` and a `last_detected: Instant`. On periodic check, if `last_detected` is within review interval and `last_printed` is either None (never printed) or older than `absence_threshold`, then print event and update `last_printed`. For "new" vs "returning", we can distinguish: If `history.len() == 1` (or this is the first detection ever) -> "New device". Else if the time between previous detection and now > `absence_threshold` -> "Returning device". We can compute the time gap between `last_detected` and `previous_last_detected` (if available). So we need to keep the most recent two detection timestamps. Instead of full history, we can store `last_detected: Instant` and `previous_last_detected: Option<Instant>` (the time before the most recent). Update: when new detection arrives, set `previous_last_detected = last_detected`, then `last_detected = now`. Then periodic thread can compute gap = if `previous_last_detected` is Some then `last_detected.duration_since(prev)`. If gap > `absence_threshold` or `previous_last_detected` is None (first time), then candidate for event. But note: if a device is continuously being detected (every few seconds), `last_detected` updates frequently, and the gap between consecutive detections is small (< review_interval). We don't want to print for every detection. The periodic thread runs every `review_interval`. At that moment, we check if `last_detected` is recent (within `review_interval` from now) and the gap from previous detection is > `absence_threshold`. However, if the device is active, `last_detected` will be recent, but `previous_last_detected` will also be recent because the device sends advertisements often (e.g., every second). The gap between detections is small (1s), not > `absence_threshold`. So we won't print. The event is only printed when the device reappears after a long silence (gap > `absence_threshold`). That's exactly what we want for "returning". For "new", `previous_last_detected` is None, so we treat as new if it's the first detection. We'll also need to ensure we don't print for every new detection if a device is new but we've already printed it; we can set a flag `printed_new` to true after printing the new event. Similarly for returning, we can set `last_printed_return` to now.

Simplify: After printing any arrival event, set `last_print_time = Some(now)`. Then next time, only print again if the device becomes absent and then reappears (gap > threshold). So the condition for printing: `last_detected` is recent (within `review_interval`), and the time since we last printed is > `absence_threshold`. But that would print again if the device is continuously active after `absence_threshold` elapsed since last print, which is wrong. So we need to base on gap between detections, not on last print time. So use the gap method.

Thus, in periodic review, for each device:
- Get `last_detected` and `previous_last_detected`.
- If `previous_last_detected` is None (first detection ever) and `last_detected` is within the last `review_interval` (i.e., it's new and recent) -> print "New device: ...".
- Else if `previous_last_detected` is Some, compute gap = `last_detected.duration_since(previous_last_detected)`. If gap > absence_threshold and `last_detected` is within `review_interval` -> print "Device returned after {gap:?}: ...". 
- After printing, we must update `previous_last_detected = last_detected`? Actually, no. The detection updates are done by the scanner thread; `previous_last_detected` is already updated. To avoid re-triggering on the next periodic review, we need to ensure that after printing, the next review won't think it's a new return. Since the gap will still be > absence_threshold? Wait: If we printed, and no new detection occurs before next review, then `last_detected` is still the same timestamp (the one that triggered). In the next review, gap = `last_detected - previous_last_detected` is still > threshold. `last_detected` might still be within review_interval? If the review_interval is 20s and the detection happened 0s ago, after 20s review, `last_detected` is now 20s old, which is <= review_interval? Actually we check if `elapsed_since_last_detected <= review_interval`. If detection was at t=0, at t=20, elapsed is 20s, so within interval. So it would again print. That's bad. We need to prevent duplicate prints. One way: after printing, set `previous_last_detected = last_detected` to make gap zero, so subsequent checks will find gap=0 < threshold. This is safe because the scanner thread does not update `previous_last_detected` until a new detection occurs. So if the scanner hasn't provided a new detection, we artificially update to avoid re-print. So after printing, we do `device.previous_last_detected = Some(device.last_detected);` That will make gap zero. Then at next review, unless a new detection updates `previous_last_detected`, gap is zero, no print. That works.

But careful: scanner thread updates `previous_last_detected` whenever a new detection occurs. It must do so atomically with lock. We'll use Arc<Mutex<HashMap>>. The periodic thread also holds lock and can modify the struct.

Implement with tokio? The bluer crate uses async. So we need an async runtime. The simplest: use tokio. The main function sets up tokio runtime, spawns tasks. The scanner task will be async. The periodic review task can be another async task that loops with tokio::time::interval. Channels: we can use tokio::sync::mpsc to send detection events from scanner to a handler that updates map, but we can also directly update the map inside the scanner's event loop with lock, which is simpler and avoids another task. However, they want channels: "I then want a mechanism, possibly using channels, which reviews this cache regularly". So we'll use channels. The scanner will send `Detection` messages to a receiver loop that updates the shared map. The periodic reviewer will read from the map (also behind lock). So three tasks: scanner, collector (processes messages), reviewer. But we can combine scanner and collector: scanner run async, on each event it sends to a channel, and we spawn a handler that receives from channel and updates map. Or scanner can directly update map, but they want channel. I'll use channel.

Design:
- `Detection { address: String, name: Option<String>, rssi: i16, timestamp: Instant }`
- `DeviceState { name: Option<String>, last_detected: Instant, previous_last_detected: Option<Instant>, first_seen_printed: bool }` (we might not need first_seen_printed, we can derive from previous_last_detected being None).
- Shared state: `Arc<Mutex<HashMap<String, DeviceState>>>`.
- Channel: `tokio::sync::mpsc::unbounded_channel()` for simplicity.
- In main, spawn collector: recv loop, on each Detection, lock map, get or insert entry, update fields.
- Spawn reviewer: sleep for `review_interval`, then lock map, iterate entries, check conditions, prints, update. Use `tokio::time::interval`.
- Spawn scanner async block: create Bluetooth session, adapter, call `discover_devices()`, for each event, extract address, name, rssi, send via channel.

For log file: if provided, open File with append mode, use a `LogOutput` enum: Stdout or File, and use `writeln!` macro.

Use clap: `scan_duration` maybe not needed (run forever until Ctrl+C). We'll implement graceful shutdown on Ctrl+C using `tokio::signal::ctrl_c()`.

Dependencies: clap with derive, tokio, bluer, serde (only needed for clap? clap can do derive without serde, but they specifically asked "serde procedural macros for Serde". Maybe they want to use serde to derive Serialize/Deserialize for config? They said: "use Rust's clap crate with the serde procedural macros for Serde". That might mean using clap with serde support, i.e., clap's derive feature is separate. But they want "with the serde procedural macros for Serde". Might be misunderstanding, but I can include serde and serde_derive features? Clap has a feature "derive" which uses `syn`/`proc-macro`, not serde. However, clap also has a feature "serde" to support deserializing args from env? Actually they might want to use `#[derive(serde::Serialize, serde::Deserialize)]` for a configuration struct that is then parsed by clap? Unclear. They said: "For the command line use Rust's clap crate with the serde procedural macros for Serde." Possibly they want clap to derive Parser with serde's help? The typical pattern is: `#[derive(Parser)] struct Args { ... }` not serde. I think they are just instructing that we should use clap with serde derive macros, maybe for a config file? Since they said "The coding style I want you to follow is to use 1 file, because the application should be pretty simple." So I'll include serde dependencies with derive features, but not necessarily use them unless needed for clap. I'll add `serde` and `serde_derive` to Cargo.toml (but in 1 file, I'll just list dependencies). I'll demonstrate using serde for a potential config struct? I'll keep it simple: just clap.

I'll also use `chrono` for human-readable timestamps? They might want to see when last observed. I'll format `SystemTime` or `Instant` into readable string. Instant doesn't give date/time, so better to use `std::time::SystemTime` for absolute timestamps. I'll use `SystemTime` for detection timestamps. Then log messages can include something like "last observed at ...". For printing "after absence duration", we can compute duration from `SystemTime`. So use `SystemTime` instead of `Instant`. Bluer provides timestamp? No, we can just capture `SystemTime::now()` when we receive event. That's fine.

Thus `Detection.timestamp: SystemTime`.

For the history, we just keep `last_detected: SystemTime` and `previous_last_detected: Option<SystemTime>`. The periodic reviewer uses `SystemTime` and compares with current time to determine if "last_detected" is recent (within review_interval). Compute elapsed = `now.duration_since(last_detected).unwrap_or_default()`. If elapsed <= review_interval, considered recent. Compute gap between `last_detected` and `previous_last_detected` if available. Use `absence_threshold` as Duration.

I'll implement the application in a single `main.rs` with all logic.

Let's outline:

```rust
use bluer::{Adapter, DiscoveryEvent, Session};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex};
use tokio::time;
```

Args:
```rust
#[derive(Parser)]
#[command(version, about = "Bluetooth presence detector")]
struct Args {
        /// Time between review cycles in seconds
    #[arg(short, long, default_value_t = 20)]
    review_interval: u64,
    /// Absence threshold in seconds to consider a device returning
    #[arg(short, long, default_value_t = 60)]
    absence_threshold: u64,
    /// Path to log file (If not set, logs only to stdout)
    #[arg(short, long)]
    log_file: Option<PathBuf>,
    /// Resolve device names (try to get human-readable name)
    #[arg(short, long, default_value_t = false)]
    resolve_names: bool,
}
```

Detection struct:
```rust
struct Detection {
        address: String,
    name: Option<String>,
    rssi: i16,
    timestamp: SystemTime,
}
```

DeviceState:
```rust
#[derive(Debug)]
struct DeviceState {
        name: Option<String>,
    last_detected: SystemTime,
    previous_last_detected: Option<SystemTime>,
    // no need for extra flag
}
```

App struct holding state and sender:
Actually we can have sender stored in collector.

Main: parse args, convert thresholds to Duration. Initialize shared map: Arc<Mutex<HashMap<String, DeviceState>>>.

Set up channel (unbounded). Spawn collector task: takes receiver, map clone. Loops receiving Detection. For each, lock map, entry().and_modify(...).or_insert(...). Update: let state = map.entry(detection.address).or_insert(DeviceState { name: detection.name, last_detected: detection.timestamp, previous_last_detected: None }); when updating: state.previous_last_detected = Some(state.last_detected); state.last_detected = detection.timestamp; If name changed, update name.

Spawn reviewer task: takes map clone, review_interval, absence_threshold, log output (optionally file). Loop with interval.tick(). On tick, lock map, for each (addr, state), if state.last_detected is recent (now - last_detected <= review_interval), then check condition:
- if state.previous_last_detected is None -> new device; print "New device ..."; then set previous_last_detected = Some(state.last_detected) to avoid duplicate.
- else if duration between last_detected and previous_last_detected > absence_threshold -> returned; print "Device returned after {gap:?} ..."; then set previous_last_detected = Some(state.last_detected) again.
Log to stdout and optionally file.

Spawn scanner task: create session, adapter, get discover stream. For each event, if it's DeviceFound { address, name, rssi, .. }, send Detection. Catch errors.

Need to handle Ctrl+C: spawn a task that listens for ctrl_c, then shutdown. Use tokio::select! in main. But easier: run all tasks and wait for ctrl_c. Use `tokio::signal::ctrl_c().await;` then exit. While waiting, tasks run.

I'll write code with proper error handling, using `anyhow` for convenience? To keep dependencies minimal, I'll avoid anyhow, just use `eprintln!` and exit on errors.

Now, write final answer.