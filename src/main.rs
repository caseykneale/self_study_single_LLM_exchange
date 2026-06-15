use anyhow::Result;
use bluer::{AdapterEvent, Session};
use chrono::{self, offset};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[derive(Debug, Clone)]
struct PresenceDetector {
    recent_detections: HashMap<String, Vec<DeviceState>>,
    grouped_devices: Vec<DetectionGroup>,
}

#[derive(Debug, Clone)]
struct DetectionGroup {
    present: bool,
    members: Vec<String>,
}

impl PresenceDetector {
    pub fn new() -> Self {
        Self {
            recent_detections: HashMap::new(),
            grouped_devices: vec![],
        }
    }

    // Add a new detection to the collection of recent detections.
    pub fn add(&mut self, detection: Detection) {
        let state = self
            .recent_detections
            .entry(detection.mac_address.clone())
            .or_insert(vec![]);
        state.push(detection.device_state);
    }

    /// Small abstraction over associating recent detections as being members of
    /// an existing group, or creating a new group.
    pub fn group_and_log(&mut self) {
        self.handle_existing_groups();
        self.handle_new_group();
    }

    /// Remove detections from a recent_detections batch that belong to existing groups.
    /// Log if a group has become present or has left.
    ///
    /// TODO: Consider making a Vec<bool> inplace of has_recent_match and breaking apart groups that have only partial matches
    /// to handle cases where multiple people enter a detectable area at roughly the same time.
    /// For my use case this isn't important.
    fn handle_existing_groups(&mut self) {
        for (group_idx, group) in self.grouped_devices.iter_mut().enumerate() {
            let has_recent_match: bool = group
                .members
                .iter()
                .any(|mac: &String| self.recent_detections.contains_key(mac));

            let was_present = group.present;
            group.present = has_recent_match;

            if group.present {
                if !was_present {
                    println!(
                        "{:?} | Group(Id: {:}) with {:} members is present",
                        chrono::offset::Local::now(),
                        group_idx,
                        group.members.len()
                    );
                }
                for member_mac in group.members.iter() {
                    if !was_present && let Some(state) = self.recent_detections.get(member_mac) {
                        let min: Option<i16> = state.iter().flat_map(|x| x.rssi).min();
                        let max: Option<i16> = state.iter().flat_map(|x| x.rssi).max();
                        println!(
                            "\t - {}: RSSI {:} - {:}, Observed: {:} times",
                            member_mac,
                            min.map_or("?".to_owned(), |v| v.to_string()),
                            max.map_or("?".to_owned(), |v| v.to_string()),
                            state.len()
                        );
                    }
                    self.recent_detections.remove(member_mac);
                }
            }

            if was_present && !group.present {
                println!(
                    "{:?} | Group(Id: {:}) with {:} members has left",
                    offset::Local::now(),
                    group_idx,
                    group.members.len()
                );
            }
        }
    }

    /// Any remaining recent_detection elements are considered a new group. Create one and log it.
    fn handle_new_group(&mut self) {
        // Create new group for detections that don't have groups
        if !self.recent_detections.is_empty() {
            let new_group = DetectionGroup {
                present: true,
                members: self.recent_detections.keys().cloned().collect(),
            };
            let group_id = self.grouped_devices.len();
            println!(
                "{:?} | New group(Id: {:}) with {:} members is present",
                chrono::offset::Local::now(),
                group_id,
                new_group.members.len()
            );
            self.grouped_devices.push(new_group);

            for (mac, state) in self.recent_detections.iter() {
                let min: Option<i16> = state.iter().flat_map(|x| x.rssi).min();
                let max: Option<i16> = state.iter().flat_map(|x| x.rssi).max();
                println!(
                    "\t - {}: RSSI {:} - {:}, Observed: {:} times",
                    mac,
                    min.map_or("?".to_owned(), |v| v.to_string()),
                    max.map_or("?".to_owned(), |v| v.to_string()),
                    state.len()
                );
            }
        }

        self.recent_detections.clear();
    }
}

#[derive(Debug, Clone)]
struct Detection {
    mac_address: String,
    device_state: DeviceState,
}

#[derive(Debug, Clone)]
struct DeviceState {
    // YAGNI: but maybe I will keep this for more advanced analysis later
    _timestamp: SystemTime,
    rssi: Option<i16>,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(version, about = "Bluetooth presence detector")]
struct Args {
    /// Absence threshold in seconds before a returning device is announced
    #[arg(short = 'i', long, default_value_t = 30)]
    poll_interval: u64,
    /// Absence threshold in seconds before a returning device is announced
    #[arg(short = 'a', long, default_value_t = 60)]
    batch_interval: u64,
}

/// Creates a Tokio task which loops over discovered Bluetooth devices
/// and obtains their 6 byte MAC address, signal strength (RSSI), and when
/// the event happened.
fn scanner(tx: UnboundedSender<Detection>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let session = Session::new().await?;
        let adapter = session.default_adapter().await?;

        adapter.set_powered(true).await?;

        let mut discover = adapter.discover_devices_with_changes().await?;
        while let Some(event) = discover.next().await {
            // Currently we ignore updates and removals and only track additions.
            if let AdapterEvent::DeviceAdded(address) = event {
                let device = adapter.device(address)?;
                let detection = Detection {
                    mac_address: address.to_string(),
                    device_state: DeviceState {
                        rssi: device.rssi().await.unwrap_or(None),
                        _timestamp: SystemTime::now(),
                    },
                };

                tx.send(detection)?;
            }
        }
        // TODO: handle reconnecting if connection is lost.
        println!("Scanner has exitted.");

        Ok(())
    })
}

/// Returns a Tokio task which recieves detections from the scanner task and updates
/// them in a shared device map data structure.
fn receiver(
    mut rx: UnboundedReceiver<Detection>,
    presence_detector: Arc<Mutex<PresenceDetector>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(detection) = rx.recv().await {
            let mut presence_detector_lock = presence_detector.lock().await;
            presence_detector_lock.add(detection);
        }
        println!("Receiver has exitted.");
    })
}

/// Returns a Tokio task that checks the current device_map state for events
/// that may indicate a person has came and went.
fn analyzer(
    arguments: Args,
    presence_detector: Arc<Mutex<PresenceDetector>>,
) -> JoinHandle<Result<()>> {
    let poll_interval = Duration::from_secs(arguments.poll_interval);

    tokio::spawn(async move {
        let mut poll_interval = tokio::time::interval(poll_interval);
        loop {
            poll_interval.tick().await;

            let mut presence_detector_lock = presence_detector.lock().await;
            presence_detector_lock.group_and_log();
        }
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let presence_detector = PresenceDetector::new();
    let presence_detector_clone = Arc::new(Mutex::new(presence_detector));

    let (tx, rx) = mpsc::unbounded_channel::<Detection>();

    let receiver = receiver(rx, presence_detector_clone.clone());
    let analyzer = analyzer(args, presence_detector_clone.clone());
    let scanner = scanner(tx);

    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    scanner.abort();
    receiver.abort();
    analyzer.abort();

    let _ = tokio::join!(scanner, receiver, analyzer);

    Ok(())
}
