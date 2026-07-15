# LAN-Drop

> Decentralized LAN file transfer, built for peer discovery, chunked transport, and integrity-first delivery without the internet.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](#author)
[![CustomTkinter](https://img.shields.io/badge/UI-CustomTkinter-00C2A8.svg)](https://github.com/TomSchimansky/CustomTkinter)
[![Networking](https://img.shields.io/badge/Networking-TCP%2FUDP-lightgrey.svg)](#system-architecture--dsa)

## Features

- Automatic peer discovery on the local network using UDP heartbeats.
- Direct device-to-device file transfer over TCP.
- Chunked transfer pipeline designed for files of any size.
- Merkle-tree-backed integrity checks for chunk-level verification.
- Responsive dark-mode GUI powered by `customtkinter`.
- Thread-safe live peer registry that updates in real time.
- Background networking workers that keep the UI responsive during hashing and transfer.

## System Architecture & DSA

LAN-Drop was designed as a networking and data-structures showcase, not just a file sender. The architecture deliberately separates discovery, transport, integrity validation, and presentation so each subsystem can stay focused and testable.

| Layer | Responsibility | Key Mechanism |
| --- | --- | --- |
| Discovery | Finds nearby peers on the same LAN | UDP heartbeat broadcast and listener threads |
| Transport | Sends and receives file data | Length-prefixed JSON control frames over TCP |
| Integrity | Validates file contents chunk by chunk | SHA-256 Merkle tree over streamed chunks |
| UI | Presents peers and transfer state | `customtkinter` main loop with background event polling |

### Merkle Trees for Data Integrity

Files are streamed in 1 MB chunks instead of being loaded into memory at once. Each chunk is hashed with SHA-256, and those hashes are assembled into a Merkle tree. The root hash represents the entire file, while each leaf represents a single chunk.

This design gives the transfer layer a strong integrity boundary:

> If a byte is corrupted in transit, the receiver rejects only the affected chunk and retries that segment rather than restarting the full transfer.

The result is a validation model that is structurally efficient and operationally resilient. The tree allows fast root comparison and chunk-level verification while preserving a small, deterministic memory footprint.

### UDP Heartbeat & Peer Discovery

Peer discovery is handled by a background UDP broadcaster on port `50000`. Each node emits a heartbeat packet containing its identity and transfer port, then listens for heartbeats from other nodes on the LAN.

The discovery service maintains a thread-safe live registry of active peers. As peers appear, disappear, or time out, the UI reflects those changes quickly without blocking the main event loop.

### O(1) Memory Footprint

LAN-Drop avoids buffering entire files in RAM. The transfer pipeline reads sequential chunks, hashes them, writes them to temporary chunk files, and streams them over TCP one chunk at a time.

That approach keeps the memory profile effectively flat relative to file size, which makes the system suitable for large multi-gigabyte transfers such as raw camera footage or 4K video.

### Asynchronous Threading

The GUI runs on the main `customtkinter` event loop, while networking work is delegated to daemon threads:

- UDP discovery broadcast thread
- UDP discovery listener thread
- TCP server listener thread
- Per-client TCP worker threads
- UI-side file send worker thread

Thread-safe queues bridge the background transfer engine and the frontend so status events can be consumed without race conditions. This keeps the interface responsive while the application hashes, validates, and transfers data.

## Prerequisites & Installation

### Prerequisites

- Python 3.10 or newer
- A local area network connection on the devices you want to use
- `pip`

### Install

```bash
git clone <your-repository-url>
cd "LAN DROP"
pip install -r requirements.txt
```

## Usage

1. Start the application on each device you want to use:

   ```bash
   python main.py
   ```

2. Wait for peers to appear in the left panel. Discovery happens automatically over UDP.

3. Click a discovered peer to select it.

4. Click **Select File** and choose the file you want to send.

5. Click **Send to Selected Peer** to begin the transfer.

6. Watch the status bar for hashing, verification, and completion updates.

> Local dual-instance testing note: the current code uses a fixed TCP transfer port of `50001` in `network/transfer.py`. To test two instances on the same machine, run them on different transfer ports. If you wrap the app with a small CLI entrypoint, expose that value as `--transfer-port`; otherwise, temporarily adjust `TRANSFER_PORT` before launching the second instance.

## Future Enhancements

- AES-256 encryption for payload confidentiality.
- Optional authentication and trusted-peer pairing.
- Multi-node swarm downloading for parallelized transfers.
- Transfer resume support after disconnects.
- Better LAN topology discovery with subnet-aware broadcast targeting.
- Persistent transfer history and integrity audit logs.

## Author

Kshitij Agarwal

## License

This project is intended to be released under the MIT License.