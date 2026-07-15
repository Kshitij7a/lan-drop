"""Application entry point for LAN-Drop."""

from __future__ import annotations

import argparse
import os
from queue import Queue

from network.discovery import PeerDiscovery
from network.transfer import TransferEvent, TransferManager
from ui.app import LANDropUI


def _parse_args() -> argparse.Namespace:
    """Parse optional runtime overrides for local smoke testing."""

    parser = argparse.ArgumentParser(description="LAN-Drop")
    parser.add_argument("--discovery-port", type=int, default=int(os.getenv("LAN_DROP_DISCOVERY_PORT", "50000")))
    parser.add_argument("--transfer-port", type=int, default=int(os.getenv("LAN_DROP_TRANSFER_PORT", "50001")))
    parser.add_argument(
        "--broadcast-target",
        action="append",
        default=None,
        help="UDP broadcast or unicast target for discovery. Can be passed multiple times.",
    )
    parser.add_argument(
        "--localhost-only",
        action="store_true",
        help="Limit discovery to localhost for single-machine smoke tests.",
    )
    return parser.parse_args()


def main() -> None:
    """Start discovery, transfer services, and the GUI."""

    args = _parse_args()
    event_queue: Queue[TransferEvent] = Queue()

    broadcast_targets = args.broadcast_target
    if args.localhost_only:
        broadcast_targets = ["127.0.0.1"]

    discovery = PeerDiscovery(
        port=args.discovery_port,
        broadcast_targets=broadcast_targets,
    )
    transfer_manager = TransferManager(port=args.transfer_port, event_queue=event_queue)

    discovery.start()
    transfer_manager.start_server()

    app = LANDropUI(discovery=discovery, transfer_manager=transfer_manager)
    app.mainloop()


if __name__ == "__main__":
    main()