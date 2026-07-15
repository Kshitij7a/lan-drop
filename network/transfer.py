"""TCP transfer engine for LAN-Drop.

The protocol uses a length-prefixed JSON control plane plus raw chunk payloads.
Each chunk carries its SHA-256 hash, and the receiver checks the hash against
the Merkle tree metadata supplied by the sender.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import socket
import struct
import tempfile
import threading
from queue import Queue
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.file_handler import CHUNK_SIZE_BYTES, ChunkManifest, FileChunk, FileProcessor
from core.merkle import MerkleTree


TRANSFER_PORT = 50001
HEADER_STRUCT = struct.Struct("!I")


def _sha256_hex(data: bytes) -> str:
    """Return the SHA-256 digest for *data* as a hexadecimal string."""
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class TransferEvent:
    """Structured event emitted to the UI or another consumer."""
    event_type: str
    message: str
    payload: Dict[str, Any]


class TransferManager:
    """Handle outgoing and incoming TCP file transfers."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = TRANSFER_PORT,
        chunk_size: int = CHUNK_SIZE_BYTES,
        event_queue: Optional[Queue[TransferEvent]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.event_queue = event_queue or Queue()
        self.file_processor = FileProcessor(chunk_size=chunk_size)

        self._server_socket: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._client_threads: List[threading.Thread] = []

    def start_server(self) -> None:
        """Start the TCP listener in a background thread."""
        if self._server_thread and self._server_thread.is_alive():
            return

        self._stop_event.clear()
        self._server_socket = self._create_server_socket()

        self._server_thread = threading.Thread(
            target=self._server_loop,
            name="lan-drop-transfer-server",
            daemon=True,
        )
        self._server_thread.start()

    def stop_server(self) -> None:
        """Stop the listener and close open sockets."""
        self._stop_event.set()

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            finally:
                self._server_socket = None

    def send_file(
        self,
        file_path: str | Path,
        peer_ip: str,
        peer_port: int = TRANSFER_PORT,
    ) -> Path:
        """Split *file_path* into chunks and send it to a peer."""
        manifest = self.file_processor.split_file(file_path)
        merkle_tree = self.file_processor.build_merkle_tree(manifest)

        metadata = {
            "type": "metadata",
            "filename": manifest.source_path.name,
            "total_size": manifest.file_size,
            "merkle_root_hash": merkle_tree.root_hash,
            "number_of_chunks": manifest.total_chunks,
            "chunk_size": manifest.chunk_size,
            "chunk_hashes": [chunk.sha256 for chunk in manifest.chunks],
        }

        try:
            with socket.create_connection((peer_ip, peer_port), timeout=10.0) as conn:
                self._send_json_packet(conn, metadata)
                ack = self._receive_json_packet(conn)
                if ack.get("type") != "ack" or ack.get("status") != "ready":
                    raise ConnectionError("Peer did not acknowledge the transfer")

                for chunk in manifest.chunks:
                    self._send_chunk(conn, manifest, chunk)
                    response = self._receive_json_packet(conn)

                    while response.get("type") == "nack" and response.get("chunk_index") == chunk.index:
                        self._send_chunk(conn, manifest, chunk)
                        response = self._receive_json_packet(conn)

                    if response.get("type") != "ack" or response.get("chunk_index") != chunk.index:
                        raise ConnectionError(f"Unexpected response for chunk {chunk.index}")

                self._send_json_packet(conn, {"type": "complete"})

        finally:
            self.file_processor.cleanup_manifest(manifest)

        self._emit_event(
            "transfer_complete",
            f"Sent {manifest.source_path.name} to {peer_ip}:{peer_port}",
            {"file": manifest.source_path.name, "peer_ip": peer_ip, "peer_port": peer_port},
        )
        return manifest.source_path

    def _create_server_socket(self) -> socket.socket:
        """Create the TCP server socket."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(8)
        server_socket.settimeout(1.0)
        return server_socket

    def _server_loop(self) -> None:
        """Accept incoming connections and hand them to worker threads."""
        while not self._stop_event.is_set():
            if self._server_socket is None:
                break

            try:
                client_socket, client_address = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            worker = threading.Thread(
                target=self._handle_client,
                args=(client_socket, client_address),
                name=f"lan-drop-transfer-client-{client_address[0]}",
                daemon=True,
            )
            self._client_threads.append(worker)
            worker.start()

    def _handle_client(self, client_socket: socket.socket, client_address: Tuple[str, int]) -> None:
        """Receive and reconstruct a file from a peer."""
        try:
            with client_socket:
                metadata = self._receive_json_packet(client_socket)
                if not metadata or metadata.get("type") != "metadata":
                    self._send_json_packet(client_socket, {"type": "nack", "reason": "missing metadata"})
                    return

                self._send_json_packet(client_socket, {"type": "ack", "status": "ready"})

                filename = str(metadata.get("filename", "unknown_file"))
                total_size = int(metadata.get("total_size", 0))
                merkle_root_hash = str(metadata.get("merkle_root_hash", ""))
                number_of_chunks = int(metadata.get("number_of_chunks", 0))
                chunk_size = int(metadata.get("chunk_size", CHUNK_SIZE_BYTES))
                
                raw_hashes = metadata.get("chunk_hashes", [])
                chunk_hashes = [str(item) for item in raw_hashes] if isinstance(raw_hashes, list) else []

                if len(chunk_hashes) != number_of_chunks:
                    self._send_json_packet(
                        client_socket,
                        {"type": "nack", "reason": "chunk hash metadata mismatch"},
                    )
                    return

                merkle_tree = MerkleTree.from_chunk_hashes(chunk_hashes)
                if merkle_tree.root_hash != merkle_root_hash:
                    self._send_json_packet(
                        client_socket,
                        {"type": "nack", "reason": "merkle root mismatch"},
                    )
                    return

                temp_dir = Path(tempfile.mkdtemp(prefix=f"lan_drop_recv_{Path(filename).stem}_"))
                output_path = temp_dir / filename

                with output_path.open("wb") as output_file:
                    output_file.truncate(total_size)

                received_chunks: List[bytes] = []

                for expected_index in range(number_of_chunks):
                    while True:
                        chunk_header = self._receive_json_packet(client_socket)
                        chunk_index_val = chunk_header.get("chunk_index") if chunk_header else None
                        
                        if not chunk_header or chunk_header.get("type") != "chunk" or chunk_index_val is None or int(chunk_index_val) != expected_index:
                            self._send_json_packet(
                                client_socket,
                                {"type": "nack", "chunk_index": expected_index, "reason": "unexpected chunk"},
                            )
                            continue

                        chunk_bytes = self._receive_bytes(client_socket, int(chunk_header.get("chunk_size", 0)))
                        received_hash = _sha256_hex(chunk_bytes)

                        if not merkle_tree.verify_chunk_hash(received_hash, expected_index):
                            self._send_json_packet(
                                client_socket,
                                {"type": "nack", "chunk_index": expected_index, "reason": "hash mismatch"},
                            )
                            continue

                        with output_path.open("r+b") as output_file:
                            output_file.seek(expected_index * chunk_size)
                            output_file.write(chunk_bytes)

                        received_chunks.append(chunk_bytes)
                        self._send_json_packet(
                            client_socket,
                            {"type": "ack", "chunk_index": expected_index, "status": "accepted"},
                        )
                        break

                final_tree = MerkleTree(received_chunks)
                if final_tree.root_hash != merkle_root_hash:
                    self._send_json_packet(
                        client_socket,
                        {"type": "nack", "reason": "final merkle verification failed"},
                    )
                    return

                self._emit_event(
                    "transfer_complete",
                    f"Received {filename} from {client_address[0]}",
                    {
                        "filename": filename,
                        "source_ip": client_address[0],
                        "output_path": str(output_path),
                        "temp_dir": str(temp_dir),
                    },
                )

                self._send_json_packet(client_socket, {"type": "complete", "status": "ok"})

        except (ConnectionError, OSError, KeyError, ValueError) as exc:
            self._emit_event("transfer_error", str(exc), {"peer": client_address[0]})

    def _send_chunk(self, conn: socket.socket, manifest: ChunkManifest, chunk: FileChunk) -> None:
        """Send a single chunk frame over the TCP connection."""
        chunk_data = self.file_processor.load_chunk(chunk)
        packet = {
            "type": "chunk",
            "chunk_index": chunk.index,
            "chunk_size": len(chunk_data),
            "chunk_hash": chunk.sha256,
            "offset": chunk.offset,
        }
        self._send_json_packet(conn, packet)
        self._send_bytes(conn, chunk_data)

    def _send_json_packet(self, conn: socket.socket, payload: Dict[str, Any]) -> None:
        """Send a JSON payload using a 4-byte length prefix."""
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        conn.sendall(HEADER_STRUCT.pack(len(raw)))
        conn.sendall(raw)

    def _receive_json_packet(self, conn: socket.socket) -> Dict[str, Any]:
        """Receive a length-prefixed JSON payload."""
        raw_length = self._receive_exact(conn, HEADER_STRUCT.size)
        (payload_length,) = HEADER_STRUCT.unpack(raw_length)
        payload = self._receive_exact(conn, payload_length)
        return json.loads(payload.decode("utf-8"))

    def _send_bytes(self, conn: socket.socket, payload: bytes) -> None:
        """Send raw bytes using a length prefix."""
        conn.sendall(struct.pack("!Q", len(payload)))
        conn.sendall(payload)

    def _receive_bytes(self, conn: socket.socket, expected_size: int) -> bytes:
        """Receive a raw byte payload with a length prefix."""
        raw_length = self._receive_exact(conn, 8)
        (payload_length,) = struct.unpack("!Q", raw_length)
        if payload_length != expected_size:
            raise ValueError("Chunk size mismatch")
        return self._receive_exact(conn, payload_length)

    def _receive_exact(self, conn: socket.socket, size: int) -> bytes:
        """Read exactly *size* bytes or raise if the connection closes."""
        buffer = bytearray()
        while len(buffer) < size:
            chunk = conn.recv(size - len(buffer))
            if not chunk:
                raise ConnectionError("Connection closed while receiving data")
            buffer.extend(chunk)
        return bytes(buffer)

    def _emit_event(self, event_type: str, message: str, payload: Dict[str, Any]) -> None:
        """Queue a structured event for the GUI or controller layer."""
        self.event_queue.put(TransferEvent(event_type=event_type, message=message, payload=payload))


__all__ = ["TRANSFER_PORT", "TransferEvent", "TransferManager"]