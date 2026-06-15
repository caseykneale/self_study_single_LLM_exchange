Write a complete Rust program that uses the [BlueR](https://crates.io/crates/bluer) crate to detect human presence via passively collected Bluetooth Low Energy advertisements. The program should scan for BLE devices, associate them into device groups (representing a person or a carried set of devices), and log when a group becomes present or leaves, along with per‑device signal strength (RSSI) statistics.

**Requirements and Design**

*Dependencies (Cargo.toml)*  
- `bluer` for Bluetooth adapter interaction.  
- `tokio` (with full features) for async runtime and channels.  
- `tokio-stream` for the stream of discovery events.  
- `clap` for CLI argument parsing.  
- `serde` (derive feature) for serializing/deserializing CLI structs (used only to derive `Serialize`/`Deserialize`).  
- `anyhow` for error handling.  
- `chrono` for human‑readable timestamps in log output.  

*CLI arguments*  
Accept two named options (both in seconds):  
- `-i` / `--poll-interval` (default 30) – how frequently the program analyses the latest batch of detections and logs presence changes.  
- `-a` / `--batch-interval` (default 60) – (defined but unused in this implementation; kept for potential future use).  
Use `clap::Parser` to define an `Args` struct.

*Data structures*

1. `Detection` – holds a device’s MAC address (as `String`) and a `DeviceState`.  
2. `DeviceState` – contains an optional `rssi: Option<i16>` and a `_timestamp: SystemTime` (captured for potential advanced analysis, not currently used).  
3. `DetectionGroup` – a group of MAC addresses that are assumed to belong to the same person/entity. It has a `present: bool` flag and a `members: Vec<String>`.  
4. `PresenceDetector` – the core state machine. It keeps:  
   - `recent_detections: HashMap<String, Vec<DeviceState>>` – ungrouped detections collected since the last analysis cycle.  
   - `grouped_devices: Vec<DetectionGroup>` – all known groups, some of which may be currently absent.  

*Async architecture (Tokio tasks)*

- **Scanner task** (`scanner`)  
  - Creates a `bluer::Session` and gets the default adapter, powers it on.  
  - Calls `adapter.discover_devices_with_changes()` to obtain a stream of `AdapterEvent`.  
  - For each `AdapterEvent::DeviceAdded(address)`, retrieves the device’s RSSI (`device.rssi().await.unwrap_or(None)`) and constructs a `Detection` with the MAC address and a `DeviceState` (RSSI and current `SystemTime::now()`).  
  - Sends the `Detection` through an unbounded `mpsc` sender.  
  - Returns a `JoinHandle<Result<()>>`.  

- **Receiver task** (`receiver`)  
  - Receives `Detection` values from an unbounded receiver.  
  - On each detection, locks the shared `PresenceDetector` (wrapped in `Arc<Mutex<>>`) and calls `presence_detector.add(detection)`.  
  - Returns a `JoinHandle<()>`.  

- **Analyzer task** (`analyzer`)  
  - Given a clone of the shared `PresenceDetector` and the parsed CLI arguments.  
  - Uses `tokio::time::interval` with the `poll_interval` duration.  
  - On each tick, locks the `PresenceDetector`, calls `group_and_log()`.  
  - Returns a `JoinHandle<Result<()>>`.  

*`PresenceDetector` methods*

- `new()` – initialises empty maps/vectors.  
- `add(detection: Detection)` – appends `detection.device_state` to the vector for `detection.mac_address` in `recent_detections`.  
- `group_and_log()` – first calls `handle_existing_groups()`, then calls `handle_new_group()`.  
- `handle_existing_groups()`:  
  - Iterates over `grouped_devices`. For each group, checks if **any** of its member MACs appear as a key in `recent_detections`.  
  - Sets `group.present` to that boolean.  
  - If the group becomes present (`present` true and was previously false):  
    - Prints a log line with timestamp (using `chrono::offset::Local::now()`), group ID (index) and number of members.  
    - For each member MAC, if it had a recent detection, prints a detail line containing the MAC, the minimum RSSI, maximum RSSI, and the number of observations. (Use `min`/`max` over the `Option<i16>` values, formatting them as a string or `?` if `None`.)  
    - Removes each member MAC from `recent_detections`.  
  - If the group leaves (`present` false and was previously true):  
    - Prints a leave message with timestamp, group ID, and member count.  
- `handle_new_group()`:  
  - If `recent_detections` is not empty after handling existing groups, create a new `DetectionGroup` with `present: true` and `members` equal to the keys of the remaining `recent_detections`.  
  - Log “New group(Id: {group_id}) with {n} members is present”.  
  - For each member MAC, log the same RSSI min/max/obs details as above.  
  - Clear `recent_detections`.  

*Main function*

- Parse CLI args.  
- Create a `PresenceDetector` and wrap it in `Arc<Mutex<>>`.  
- Create an unbounded channel for `Detection`.  
- Spawn the scanner, receiver, and analyzer tasks.  
- Wait for `ctrl_c` signal.  
- Abort all three tasks and await their `JoinHandle`s.  
- Exit.

*Expected behaviour*  
Every `poll_interval` seconds, the program prints group presence changes and the RSSI statistics of the involved devices. Any devices seen in the same interval that are not part of an existing group become a new group. An existing group is considered present as long as at least one of its members is seen in that interval – it is reported as leaving when none are seen. The output format must match the examples shown in the supplied code (timestamps, group IDs, “is present” / “has left”, member details with RSSI min‑max and observation count).

**Instructions for the LLM**  

Generate the full Rust source code (single file or modules) that implements exactly the above design. The code must be functionally equivalent to the provided reference, but you may choose your own variable names, code organisation, and exact formatting style (as long as the output format is preserved). Ensure that the program compiles and runs with the specified dependencies. Include all necessary `Cargo.toml` content in your answer (or as a separate block).

# REASONING
We need to write a detailed prompt instructing another LLM to create this project. The prompt should be about designing the software, not necessarily replicating variable names, but should produce functionally equivalent code. We must include all key aspects: using bluer crate for Bluetooth LE discovery, detecting presence based on MAC addresses, grouping devices into groups (likely representing persons), using RSSI for signal strength, using Tokio async, unbounded channels, shared state with Arc<Mutex<>>, CLI arguments with clap, etc.

The prompt should be high-level design description, specifying the structure, behavior, and components. It should mention the use of BlueR to scan for BLE advertisements, detect device additions, collect RSSI and timestamp, batch and group devices to infer presence, and log when groups appear or disappear. Must describe the two intervals: poll_interval for batching and logging, and batch_interval (though the code uses poll_interval as the interval for analyzer to call group_and_log; it doesn't use batch_interval, but it's defined in Args. Actually the code sets poll_interval used for analyzer's tick. batch_interval is defined but not used; maybe it was intended but not used. We'll keep the args as defined, but the batch_interval is present; might be a bug, but we should mention the existence of both parameters. The prompt should ask for CLI args: --poll-interval (default 30) for how often to check presence and print status, and --absence-threshold or batch-interval (default 60) maybe for how long to wait before declaring a group absent? In the code, group_and_log is called every poll_interval seconds; group presence is determined by any recent detection within that interval (since recent_detections are cleared after each grouping). So absence is after one poll_interval cycle without detection. So batch_interval is unused. The prompt should reflect that the batch_interval may be intended for something else, but we can just describe the design as given.

We need to describe the PresenceDetector struct with its methods: add, group_and_log, handle_existing_groups, handle_new_group. The idea: recent_detections is a map from MAC to vector of DeviceState; after each poll interval, we check existing groups: if any member has a recent detection, group is present and we log min/max RSSI and count, and remove all members from recent_detections. If a group is not present but was, log leaving. After processing existing groups, any remaining detections in recent_detections form a new group, we create a new group with those MACs, log presence, and then clear recent_detections. This is a simple heuristic: devices seen together in same batch are assumed to belong to same person/device set.

We need to describe the async architecture: scanner task sends Detections via unbounded channel to receiver task, which adds to shared PresenceDetector behind Arc<Mutex<>>. Analyzer task periodically locks the detector, calls group_and_log.

We also need to describe error handling with anyhow, use of chrono for timestamps, serde for CLI (though unused). Use of tokio::signal::ctrl_c for graceful shutdown, cancelling tasks.

The prompt should be structured to instruct the model to write the Rust program. Should include: project setup with Cargo.toml dependencies (bluer, tokio, clap, serde, anyhow, chrono). Design the crate as a binary.

We should be detailed about how Bluetooth detection works: session, adapter, device discovery, RSSI, MAC address. The scanner should listen for DeviceAdded events, create Detection with address and DeviceState (rssi, timestamp). RSSI might be None if not available.

We need to note that the code sometimes references `batch_interval` as an argument but uses `poll_interval` in analyzer. The prompt can mention that there are two configurable intervals but in the provided implementation one is unused; the user may incorporate both. We'll describe both as: poll_interval (how often to check presence) and batch_interval (maybe intended as a window length, but in this code it's not used). We can mention that the grouping logic uses the most recent batch of detections within the poll interval to determine presence, which implicitly sets an absence timeout equal to poll interval. However, the user might want to implement a separate batch_interval; we'll instruct to implement exactly as given, so batch_interval is defined but unused; that's fine.

We should ensure the generated code is functionally equivalent, meaning same behavior: printing to stdout with formatted messages like "Group(Id: X) with Y members is present", listing each member with RSSI min-max and observed count, and when groups leave. And the timing.

We should ask the model to produce a complete Rust program with async main, using Tokio.

We'll write a prompt that says: "Write a Rust program that uses the BlueR crate to detect the presence of people based on Bluetooth signals. The program should..." and then list all requirements.

We'll start with a brief intro, then specify dependencies, then describe the data structures, then the async tasks, then the main logic, then the behavior. We'll include guidance on how to structure the code. We'll emphasize that the solution should be functionally equivalent to the given code, but exact naming can differ.

We'll ensure to include the grouping logic details: how groups are formed and tracked, how presence is logged. Also include CLI args.

We should also note that the code uses `chrono::offset::Local::now()` for timestamps, not the device's timestamp. The `_timestamp` field in DeviceState is recorded but not used; we can mention it's for potential future analysis.