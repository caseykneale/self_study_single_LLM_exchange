```rust
use bluer::{AdapterEvent, Session};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;

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
        .map(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
        })
        .transpose()
        .map_err(|e| {
            bluer::Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to open log file: {}", e),
            ))
        })?;

    // Shared device map
    let devices: SharedState = Arc::new(Mutex::new(HashMap::new()));

    // Unbounded channel for Detections
    let (tx, mut rx) = mpsc::unbounded_channel::<Detection>();

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
                let elapsed = now.duration_since(state.last_detected).unwrap_or_default();
                if elapsed > review_interval {
                    continue;
                }

                let name_display = state.name.as_deref().unwrap_or("<unknown>");
                let rssi_hint = ""; // RSSI not stored here; could be added

                // First‑time detection (no previous record)
                if state.previous_last_detected.is_none() {
                    let msg = format!(
                        "[NEW] Device {} ({}) appeared for the first time at {:?}.",
                        name_display, addr, state.last_detected
                    );
                    log_and_print(&msg, &mut log_file);
                    // Prevent re‑reporting until it disappears and returns
                    state.previous_last_detected = Some(state.last_detected);
                } else {
                    let prev = state.previous_last_detected.unwrap();
                    let gap = state.last_detected.duration_since(prev).unwrap_or_default();

                    // Has the device been absent long enough?
                    if gap > absence_threshold {
                        let msg = format!(
                            "[RETURN] Device {} ({}) returned after {:?}. Last seen {:?}.",
                            name_display, addr, gap, prev
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
        while let Some(event) = discover.next().await {
            match event {
                AdapterEvent::DeviceAdded(address) => {
                    let device = adapter.device(address).unwrap();

                    let detection = Detection {
                        address: address.to_string(),
                        name: if args.resolve_names {
                            device.name().await.unwrap()
                        } else {
                            None
                        },
                        rssi: device.rssi().await.unwrap_or(Some(0)).unwrap_or(0),
                        timestamp: SystemTime::now(),
                    };

                    println!(
                        "t: {:?}, class {:?}",
                        detection,
                        device.class().await.unwrap_or(Some(0)).unwrap_or(0)
                    );

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
    if let Some(file) = log_file {
        let _ = writeln!(file, "{}", msg);
    }
}
```