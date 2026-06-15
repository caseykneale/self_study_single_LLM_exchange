```rust
use anyhow::Result;
use bluer::{AdapterEvent, Session};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

type SharedState = Arc<Mutex<HashMap<String, DeviceState>>>;

#[derive(Debug, Clone)]
struct Detection {
    mac_address: String,
    rssi: i16,
    timestamp: SystemTime,
}

#[derive(Debug)]
struct DeviceState {
    last_detected: SystemTime,
    previous_last_detected: Option<SystemTime>,
    rssi: i16,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(version, about = "Bluetooth presence detector")]
struct Args {
    /// Absence threshold in seconds before a returning device is announced
    #[arg(short = 'i', long, default_value_t = 60)]
    poll_interval: u64,
    /// Absence threshold in seconds before a returning device is announced
    #[arg(short = 'a', long, default_value_t = 600)]
    absence_threshold: u64,
}

/// Creates a Tokio task which loops over discovered Bluetooth devices
/// and obtains their 6 byte MAC address, signal strength (RSSI), and when
/// the event happened.
fn scanner(tx: UnboundedSender<Detection>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let session = Session::new().await?;
        let adapter = session.default_adapter().await?;

        adapter.set_powered(true).await?;

        let mut discover = adapter.discover_devices().await?;
        while let Some(event) = discover.next().await {
            // Currently we ignore updates and removals in favor of tracking additions.
            if let AdapterEvent::DeviceAdded(address) = event {
                let device = adapter.device(address)?;
                let detection = Detection {
                    mac_address: address.to_string(),
                    rssi: device.rssi().await.unwrap_or(Some(0)).unwrap_or(0),
                    timestamp: SystemTime::now(),
                };

                tx.send(detection)?;
            }
        }
        Ok(())
    })
}

/// Returns a Tokio task which recieves detections from the scanner task and updates
/// them in a shared device map data structure.
fn receiver(mut rx: UnboundedReceiver<Detection>, device_map: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(detection) = rx.recv().await {
            let mut map = device_map.lock().await;
            let state = map
                .entry(detection.mac_address.clone())
                .or_insert(DeviceState {
                    last_detected: detection.timestamp,
                    previous_last_detected: None,
                    rssi: detection.rssi,
                });

            state.previous_last_detected = Some(state.last_detected);
            state.last_detected = detection.timestamp;
        }
    })
}

/// Returns a Tokio task that checks the current device_map state for events
/// that may indicate a person has came and went.
fn analyzer(arguments: Args, device_map: SharedState) -> JoinHandle<Result<()>> {
    let poll_interval = Duration::from_secs(arguments.poll_interval);
    let absence_threshold = Duration::from_secs(arguments.absence_threshold);

    tokio::spawn(async move {
        let mut poll_interval = tokio::time::interval(poll_interval);
        loop {
            poll_interval.tick().await;
            let mut map = device_map.lock().await;

            for (addr, state) in map.iter_mut() {
                if let Some(prev) = state.previous_last_detected {
                    let gap = state.last_detected.duration_since(prev).unwrap_or_default();

                    if gap > absence_threshold {
                        println!(
                            "[RETURN] Device {} returned after {:?}. Last seen {:?}, RSSI {:}.",
                            addr, gap, prev, state.rssi
                        );

                        state.previous_last_detected = Some(state.last_detected);
                    }
                } else {
                    println!(
                        "[NEW] Device {} appeared for the first time at {:?}, RSSI {:}.",
                        addr, state.last_detected, state.rssi
                    );

                    state.previous_last_detected = Some(state.last_detected);
                }
            }
        }
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let device_map: SharedState = Arc::new(Mutex::new(HashMap::new()));

    let (tx, rx) = mpsc::unbounded_channel::<Detection>();

    let receiver = receiver(rx, device_map.clone());
    let analyzer = analyzer(args, device_map.clone());
    let scanner = scanner(tx);

    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    scanner.abort();
    receiver.abort();
    analyzer.abort();

    let _ = tokio::join!(scanner, receiver, analyzer);

    Ok(())
}
```