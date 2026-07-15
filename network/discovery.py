"""UDP peer discovery for LAN-Drop.

This module broadcasts periodic heartbeats and listens for peer heartbeats on
the local network. The peer registry is protected by a lock so it can be read
from the GUI thread without race conditions.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
import platform
import socket
import threading
import time
import uuid
from typing import Dict, Iterable, List, Optional


DISCOVERY_PORT = 50000
DISCOVERY_MAGIC = "LAN_DROP_HEARTBEAT"


@dataclass(frozen=True)
class PeerInfo:
    """Live peer metadata tracked by the discovery service."""

    hostname: str
    ip_address: str
    last_seen: float
    port: int


class PeerDiscovery:
    """Broadcast and receive UDP heartbeats for nearby LAN peers."""

    def __init__(
        self,
        port: int = DISCOVERY_PORT,
        broadcast_interval: float = 3.0,
        stale_timeout: float = 15.0,
        broadcast_targets: Optional[Iterable[str]] = None,
        peer_port: int = 50001,
    ) -> None:
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.stale_timeout = stale_timeout
        self.broadcast_targets = list(broadcast_targets or ["255.255.255.255"])
        self.peer_port = peer_port

        self._peer_id = str(uuid.uuid4())
        self._hostname = socket.gethostname()
        self._peers: Dict[str, PeerInfo] = {}
        self._peers_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._socket_lock = threading.Lock()
        self._socket: socket.socket | None = None

    @property
    def peers(self) -> Dict[str, PeerInfo]:
        """Return a snapshot of active peers keyed by IP address."""

        with self._peers_lock:
            return dict(self._peers)

    def start(self) -> None:
        """Start heartbeat broadcast and receive loops."""

        if self._threads:
            return

        self._stop_event.clear()
        self._socket = self._create_socket()

        broadcaster = threading.Thread(
            target=self._broadcast_loop,
            name="lan-drop-discovery-broadcast",
            daemon=True,
        )
        listener = threading.Thread(
            target=self._listen_loop,
            name="lan-drop-discovery-listen",
            daemon=True,
        )
        reaper = threading.Thread(
            target=self._cleanup_loop,
            name="lan-drop-discovery-reaper",
            daemon=True,
        )

        self._threads = [broadcaster, listener, reaper]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        """Stop background work and close the UDP socket."""

        self._stop_event.set()

        with self._socket_lock:
            if self._socket is not None:
                try:
                    self._socket.close()
                finally:
                    self._socket = None

        self._threads.clear()

    def _create_socket(self) -> socket.socket:
        """Create a UDP socket suitable for broadcast and local listening."""

        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if hasattr(socket, "SO_REUSEPORT"):
            try:
                udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1) # type: ignore
            except OSError:
                pass

        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_socket.bind(("", self.port))
        udp_socket.settimeout(1.0)
        return udp_socket

    def _broadcast_loop(self) -> None:
        """Broadcast a heartbeat packet at a fixed interval."""

        while not self._stop_event.is_set():
            packet = self._build_heartbeat_packet()
            with self._socket_lock:
                udp_socket = self._socket

            if udp_socket is None:
                break

            payload = json.dumps(packet, separators=(",", ":")).encode("utf-8")
            for target in self._resolve_broadcast_targets():
                try:
                    udp_socket.sendto(payload, (target, self.port))
                except OSError:
                    continue

            self._stop_event.wait(self.broadcast_interval)

    def _listen_loop(self) -> None:
        """Listen for peer heartbeats and refresh the peer registry."""

        while not self._stop_event.is_set():
            with self._socket_lock:
                udp_socket = self._socket

            if udp_socket is None:
                break

            try:
                data, address = udp_socket.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break

            self._handle_packet(data, address[0])

    def _cleanup_loop(self) -> None:
        """Remove peers that have not been seen recently."""

        while not self._stop_event.is_set():
            now = time.time()
            with self._peers_lock:
                stale_ips = [
                    ip
                    for ip, peer in self._peers.items()
                    if now - peer.last_seen > self.stale_timeout
                ]
                for ip in stale_ips:
                    self._peers.pop(ip, None)

            self._stop_event.wait(1.0)

    def _handle_packet(self, data: bytes, sender_ip: str) -> None:
        """Parse a heartbeat packet and update the peer registry."""

        try:
            packet = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return

        if packet.get("magic") != DISCOVERY_MAGIC:
            return

        if packet.get("peer_id") == self._peer_id:
            return

        hostname = str(packet.get("hostname") or sender_ip)
        peer_port = int(packet.get("port") or self.peer_port)

        with self._peers_lock:
            self._peers[sender_ip] = PeerInfo(
                hostname=hostname,
                ip_address=sender_ip,
                last_seen=time.time(),
                port=peer_port,
            )

    def _build_heartbeat_packet(self) -> dict:
        """Create a broadcast heartbeat payload."""

        return {
            "magic": DISCOVERY_MAGIC,
            "peer_id": self._peer_id,
            "hostname": self._hostname,
            "port": self.peer_port,
            "platform": platform.system(),
            "timestamp": time.time(),
        }

    def _resolve_broadcast_targets(self) -> List[str]:
        """Return a broadcast target list that works on constrained networks.

        Users can inject subnet-specific broadcast addresses such as
        ``192.168.1.255`` for environments where the universal broadcast address
        is filtered.
        """

        targets = list(self.broadcast_targets)
        if "255.255.255.255" not in targets:
            targets.append("255.255.255.255")
        return targets


__all__ = ["DISCOVERY_PORT", "DISCOVERY_MAGIC", "PeerDiscovery", "PeerInfo"]