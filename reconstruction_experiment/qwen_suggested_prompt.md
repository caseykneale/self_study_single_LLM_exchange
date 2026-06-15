Here is a highly detailed, architecturally-focused prompt designed to guide another LLM in generating a functionally equivalent Rust project. It emphasizes system design, state flow, and async patterns while leaving room for idiomatic refactoring.

---

### Prompt for LLM Code Generation

**Role & Objective**
You are an expert Rust systems programmer. Your task is to generate a complete, production-ready Rust project that implements a Bluetooth-based presence detector using the `bluer` crate. The output must be functionally equivalent to the reference implementation in terms of system behavior, data flow, state management, and console output format. You are free to refactor module organization, rename variables, and improve comments, but the core logic, async architecture, and observable behavior must remain identical.

**System Architecture**
Design an asynchronous, multi-task pipeline running on the `tokio` runtime with the following components:
1. **Scanner Task**: Monitors the Bluetooth adapter for new device advertisements.
2. **Processor/Receiver Task**: Consumes raw detections and appends them to shared state.
3. **Analyzer Task**: Runs on a configurable interval, evaluates presence groups, and logs events.
4. **Shared State**: A `PresenceDetector` struct wrapped in `Arc<Mutex<>>`, accessible by the Processor and Analyzer.
5. **Communication**: Tasks communicate via `tokio::sync::mpsc::unbounded_channel`.

**Core Data Models**
Define the following types with appropriate derives and semantics:
- `DeviceState`: Tracks a single Bluetooth observation. Contains:
  - `mac_address: String`
  - `rssi: Option<i16>`
  - `observed_at: SystemTime` (or equivalent)
- `Detection`: A wrapper containing `mac_address: String` and `device_state: DeviceState`.
- `DetectionGroup`: Represents a logical entity (e.g., a person) composed of multiple devices. Contains:
  - `is_present: bool`
  - `members: Vec<String>` (MAC addresses)
- `PresenceDetector`: Central state manager. Contains:
  - `recent_detections: HashMap<String, Vec<DeviceState>>` (MAC -> history of observations in the current batch)
  - `active_groups: Vec<DetectionGroup>`

**Presence Detection & Grouping Algorithm**
The analyzer must implement a batch-processing cycle that executes on each interval tick. The exact logical flow must be preserved:
1. **Process Existing Groups**:
   - Iterate through `active_groups`.
   - For each group, check if any member MAC exists as a key in `recent_detections`.
   - If a match exists:
     - Set `is_present = true`.
     - If the group was previously absent, log an arrival event in the format:
       `[TIMESTAMP] | Group(Id: <index>) with <count> members is present`
     - For each matched member, compute and log min/max RSSI and observation count in the format:
       `	 - <MAC>: RSSI <min> - <max>, Observed: <count> times`
     - Remove the matched MAC from `recent_detections`.
   - If no members match:
     - Set `is_present = false`.
     - If the group was previously present, log a departure event in the format:
       `[TIMESTAMP] | Group(Id: <index>) with <count> members has left`
2. **Handle New Detections**:
   - If `recent_detections` is not empty after processing existing groups, create a new `DetectionGroup` with `is_present = true`, containing all remaining MACs as members.
   - Log a new group arrival event in the same format as above.
   - Log per-member RSSI statistics for the new group members.
3. **Clear Batch State**:
   - Clear `recent_detections` to prepare for the next interval.

**Async Task Design**
- **Scanner**:
  - Initialize `bluer::Session`, retrieve the default adapter, and power it on.
  - Start device discovery using `adapter.discover_devices_with_changes()`.
  - Filter exclusively for `AdapterEvent::DeviceAdded`. Ignore removals and updates.
  - For each added device, fetch RSSI asynchronously via `device.rssi()`, construct a `Detection`, and send it through an `UnboundedSender`.
- **Processor**:
  - Accept an `UnboundedReceiver<Detection>` and `Arc<Mutex<PresenceDetector>>`.
  - Continuously `recv()` detections, lock the detector, and append to `recent_detections`.
- **Analyzer**:
  - Accept CLI arguments and the shared detector state.
  - Spawn a `tokio::time::interval` using the `poll_interval` argument.
  - On each tick, lock the detector and invoke the grouping algorithm.

**CLI & Configuration**
Use `clap` to define the following arguments:
- `--poll-interval` / `-i`: Seconds between analysis cycles (default: `30`)
- `--batch-interval` / `-a`: Seconds (default: `60`) *[Note: This is reserved for future batch windowing and does not need to alter current logic, but must be parsed and stored.]*

**Error Handling & Lifecycle**
- Use `anyhow::Result` for error propagation.
- Wrap async task spawns in proper error handling.
- Listen for `Ctrl+C` via `tokio::signal::ctrl_c()`.
- Upon signal: print `"Shutting down..."`, abort all three tasks via `.abort()`, join them concurrently, and exit successfully.

**Technical Constraints & Output Requirements**
- Provide a complete `Cargo.toml` with all required dependencies: `bluer`, `tokio` (with `full` features), `tokio-stream`, `clap`, `chrono`, `serde`, `anyhow`.
- Provide a single, well-structured `src/main.rs` (or clearly separated modules if preferred, but keep imports self-contained).
- Ensure idiomatic Rust practices: proper async/await usage, `Mutex` locking patterns, channel lifetime management, and `bluer` API compliance.
- Console output timestamps must use `chrono::Local`.
- Do not replicate exact variable names or comments, but preserve the exact logical flow, state transitions, grouping behavior, and output formatting.
- The code must compile cleanly with `rustc 1.75+` and run on Linux with Bluetooth BlueZ support.

**Deliverable**
Return the complete `Cargo.toml` and source code. Include brief architectural notes if necessary, but prioritize a ready-to-build, functionally equivalent implementation.