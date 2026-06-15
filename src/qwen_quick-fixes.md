```rust
use bluer::{AdapterProperty, Address, Session};
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

//might need to be tokio
use tokio_stream::StreamExt;

/// Command line interface with clap and serde procedural macros
#[derive(Parser, Debug, Clone, Deserialize)]
#[command(version, about = "Bluetooth Signal Tracker")]
struct Cli {
    /// Seconds between correlation checks
    #[arg(short, long, default_value = "20")]
    check_interval_secs: u64,

    /// Path to the verbose log file
    #[arg(short, long, default_value = "bt_tracker.log")]
    log_file: String,

    /// Seconds to wait for Bluetooth advertisements per scan cycle
    #[arg(short, long, default_value = "10")]
    scan_timeout_secs: u64,
}

/// Stores a single detection event
#[derive(Debug, Clone)]
struct SignalEntry {
    timestamp: SystemTime,
    rssi: i16,
}

/// Thread-safe cache: Unique ID -> Ordered list of detections over time
type DeviceCache = Mutex<HashMap<Address, Vec<SignalEntry>>>;

/// Simple logging function that writes to both stdout and a file
fn log_event(msg: &str, log_path: &str) {
    println!("{}", msg);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = writeln!(file, "{}", msg);
    }
}

fn main() {
    let cli = Cli::parse();
    let cache = Arc::new(DeviceCache::new(HashMap::new()));
    let (tx, rx) = mpsc::channel();

    // 1. Scanner Thread: Listens for BT signals and pushes raw data to the channel
    let scan_timeout = Duration::from_secs(cli.scan_timeout_secs);
    thread::spawn(async move || {
        let session = Session::new().await.unwrap();
        let adapter = session.default_adapter().await.unwrap();

        loop {
            match adapter.discover_devices().await {
                Ok(mut devices) => {
                    while let Some(dev) = devices.next().await {
                        match dev {
                            bluer::AdapterEvent::DeviceAdded(addr) => {
                                let device = adapter.device(addr).unwrap();
                                if let Ok(Some(rssi)) = device.rssi().await {
                                    println!("Device found [{}] - RSSI: {} dBm", addr, rssi);
                                    let entry = SignalEntry {
                                        timestamp: SystemTime::now(),
                                        rssi,
                                    };
                                    tx.send((addr, entry)).expect("Channel send failed");
                                }
                            }
                            bluer::AdapterEvent::PropertyChanged(adapter_property) => {
                                // todo
                            }
                            bluer::AdapterEvent::DeviceRemoved(address) => {
                                // todo
                            }
                        }
                    }
                }
                Err(e) => {
                    log_event(
                        &format!("WARNING: Discovery error: {}", e),
                        &cli.log_file.clone(),
                    );
                    thread::sleep(Duration::from_millis(1000));
                }
            }
        }
    });

    // 2. Coordinator/Reviewer Thread: Updates cache, runs correlation logic
    let log_path = cli.log_file.clone();
    let check_interval = Duration::from_secs(cli.check_interval_secs);
    let mut seen_recently: HashMap<Address, SystemTime> = HashMap::new();
    let mut cycle_count = 0;

    loop {
        // Process channel messages non-blocking
        let mut processed = false;
        while let Ok((addr, entry)) = rx.try_recv() {
            processed = true;
            let mut cache = cache.lock().unwrap();
            cache.entry(addr).or_default().push(entry);
            // Keep memory usage bounded (keep last 500 entries per device)
            if cache.get_mut(&addr).unwrap().len() > 1000 {
                let entries = cache.get_mut(&addr).unwrap();
                entries.drain(..500);
            }
        }
        if !processed {
            thread::sleep(Duration::from_millis(50));
        }

        // Periodic correlation review
        thread::sleep(check_interval);
        cycle_count += 1;
        let now = SystemTime::now();

        let cache = cache.lock().unwrap();
        let mut newly_active = Vec::new();
        let mut newly_inactive = Vec::new();

        for (addr, entries) in cache.iter() {
            if let Some(last) = entries.last() {
                if let Ok(elapsed) = now.duration_since(last.timestamp) {
                    if elapsed <= Duration::from_secs(cli.check_interval_secs + 5) {
                        // Still active within window
                        if !seen_recently.contains_key(addr) {
                            newly_active.push((addr, *last));
                        }
                    } else {
                        // Expired from window
                        if seen_recently.contains_key(addr) {
                            newly_inactive.push((addr, *last));
                        }
                    }
                }
            }
        }

        // Update tracking state & log events
        for (addr, entry) in &newly_active {
            seen_recently.insert(**addr, entry.timestamp);
            let ts = entry
                .timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            log_event(
                &format!(
                    "[ARRIVAL] New signature: {:?} | RSSI: {} dBm | First seen: {}s",
                    addr,
                    entry.rssi,
                    ts.as_secs()
                ),
                &log_path,
            );
        }

        for (addr, entry) in &newly_inactive {
            seen_recently.remove(addr);
            let ts = entry
                .timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            log_event(
                &format!(
                    "[DEPARTURE] Lost signature: {:?} | Last seen: {}s",
                    addr,
                    ts.as_secs()
                ),
                &log_path,
            );
        }

        if cycle_count % 10 == 0 {
            log_event(
                &format!(
                    "[STATUS] Cycle {}. Active signatures: {}",
                    cycle_count,
                    seen_recently.len()
                ),
                &log_path,
            );
        }
    }
}
```