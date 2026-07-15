"""CustomTkinter user interface for LAN-Drop.

The UI stays responsive by polling thread-safe queues and by pushing any file
hashing or transfer work into background threads.
"""

from __future__ import annotations

from pathlib import Path
import threading
from queue import Empty
from typing import Dict, Optional

import customtkinter as ctk
from tkinter import filedialog, messagebox

from network.discovery import PeerDiscovery, PeerInfo
from network.transfer import TransferEvent, TransferManager


class LANDropUI(ctk.CTk):
    """Main application window for LAN-Drop."""

    def __init__(
        self,
        discovery: PeerDiscovery,
        transfer_manager: TransferManager,
    ) -> None:
        super().__init__()

        self.discovery = discovery
        self.transfer_manager = transfer_manager

        self.selected_file: Optional[Path] = None
        self.selected_peer_ip: Optional[str] = None
        self._peer_buttons: Dict[str, ctk.CTkButton] = {}
        self._last_peer_snapshot: Dict[str, PeerInfo] = {}

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.title("LAN-Drop")
        self.geometry("1180x720")
        self.minsize(980, 620)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_layout()
        self.after(150, self._poll_events)
        self.after(500, self._refresh_peers)

    def _build_layout(self) -> None:
        """Create the main three-panel layout."""

        self.grid_columnconfigure(0, weight=1, uniform="main")
        self.grid_columnconfigure(1, weight=2, uniform="main")
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=0)

        self.left_frame = ctk.CTkFrame(self, corner_radius=16)
        self.left_frame.grid(row=0, column=0, sticky="nsew", padx=(16, 8), pady=(16, 8))
        self.left_frame.grid_rowconfigure(1, weight=1)
        self.left_frame.grid_columnconfigure(0, weight=1)

        left_title = ctk.CTkLabel(
            self.left_frame,
            text="Discovered Peers",
            font=ctk.CTkFont(size=18, weight="bold"),
        )
        left_title.grid(row=0, column=0, sticky="w", padx=16, pady=(16, 8))

        self.peer_scroll = ctk.CTkScrollableFrame(self.left_frame, corner_radius=12)
        self.peer_scroll.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self.peer_scroll.grid_columnconfigure(0, weight=1)

        self.peer_hint = ctk.CTkLabel(
            self.peer_scroll,
            text="Waiting for peers on the LAN...",
            text_color="#9AA4B2",
        )
        self.peer_hint.grid(row=0, column=0, sticky="w", padx=12, pady=12)

        self.right_frame = ctk.CTkFrame(self, corner_radius=16)
        self.right_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 16), pady=(16, 8))
        self.right_frame.grid_columnconfigure(0, weight=1)
        self.right_frame.grid_rowconfigure(0, weight=1)
        self.right_frame.grid_rowconfigure(1, weight=0)
        self.right_frame.grid_rowconfigure(2, weight=0)

        transfer_card = ctk.CTkFrame(self.right_frame, corner_radius=18)
        transfer_card.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
        transfer_card.grid_columnconfigure(0, weight=1)
        transfer_card.grid_rowconfigure(0, weight=1)
        transfer_card.grid_rowconfigure(1, weight=0)

        self.select_button = ctk.CTkButton(
            transfer_card,
            text="Select File",
            height=48,
            command=self._select_file,
        )
        self.select_button.grid(row=0, column=0, sticky="s", padx=28, pady=(0, 16))

        self.file_label = ctk.CTkLabel(
            transfer_card,
            text="No file selected",
            font=ctk.CTkFont(size=16, weight="bold"),
            wraplength=520,
        )
        self.file_label.grid(row=1, column=0, sticky="n", padx=24, pady=(0, 10))

        self.file_size_label = ctk.CTkLabel(
            transfer_card,
            text="",
            text_color="#9AA4B2",
        )
        self.file_size_label.grid(row=2, column=0, sticky="n", padx=24, pady=(0, 18))

        self.send_button = ctk.CTkButton(
            self.right_frame,
            text="Send to Selected Peer",
            height=42,
            command=self._send_selected_file,
        )
        self.send_button.grid(row=1, column=0, sticky="ew", padx=20, pady=(0, 12))

        self.selection_label = ctk.CTkLabel(
            self.right_frame,
            text="No peer selected",
            text_color="#9AA4B2",
        )
        self.selection_label.grid(row=2, column=0, sticky="w", padx=24, pady=(0, 20))

        self.bottom_frame = ctk.CTkFrame(self, corner_radius=16)
        self.bottom_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=16, pady=(0, 16))
        self.bottom_frame.grid_columnconfigure(0, weight=1)

        self.progress_label = ctk.CTkLabel(self.bottom_frame, text="Idle")
        self.progress_label.grid(row=0, column=0, sticky="w", padx=18, pady=(14, 4))

        self.progress_bar = ctk.CTkProgressBar(self.bottom_frame)
        self.progress_bar.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 8))
        self.progress_bar.set(0.0)

        self.status_label = ctk.CTkLabel(
            self.bottom_frame,
            text="Ready",
            text_color="#9AA4B2",
        )
        self.status_label.grid(row=2, column=0, sticky="w", padx=18, pady=(0, 14))

    def _select_file(self) -> None:
        """Open a file picker and show the selected file metadata."""

        filename = filedialog.askopenfilename(title="Select file to send")
        if not filename:
            return

        file_path = Path(filename)
        self.selected_file = file_path
        self.file_label.configure(text=file_path.name)
        self.file_size_label.configure(text=self._format_size(file_path.stat().st_size))
        self._set_status(f"Selected {file_path.name}")

    def _send_selected_file(self) -> None:
        """Send the currently selected file to the active peer in the background."""

        if self.selected_file is None:
            messagebox.showinfo("LAN-Drop", "Select a file before sending.")
            return

        if self.selected_peer_ip is None:
            messagebox.showinfo("LAN-Drop", "Select a peer from the left panel first.")
            return

        peer = self.discovery.peers.get(self.selected_peer_ip)
        if peer is None:
            messagebox.showerror("LAN-Drop", "The selected peer is no longer available.")
            self.selected_peer_ip = None
            self.selection_label.configure(text="No peer selected")
            return

        self._set_progress(0.0, "Hashing chunks...")
        self.send_button.configure(state="disabled")

        worker = threading.Thread(
            target=self._send_worker,
            args=(self.selected_file, peer),
            daemon=True,
        )
        worker.start()

    def _send_worker(self, file_path: Path, peer: PeerInfo) -> None:
        """Hash and send a file without blocking the UI thread."""

        try:
            self.after(
                0,
                lambda: self._set_progress(0.1, "Hashing chunks...")
            )

            # Reuse the transfer manager so the TCP protocol stays centralized.
            self.transfer_manager.send_file(file_path, peer.ip_address, peer.port)

            self.after(
                0,
                lambda: self._set_progress(1.0, "Verified successfully")
            )
        except Exception as exc:  # pragma: no cover - defensive UI boundary
            self.after(0, lambda: self._set_status(f"Transfer failed: {exc}"))
            self.after(0, lambda: self._set_progress(0.0, "Idle"))
        finally:
            self.after(0, lambda: self.send_button.configure(state="normal"))

    def _refresh_peers(self) -> None:
        """Refresh the peer panel from the live discovery registry."""

        peers = self.discovery.peers
        if peers != self._last_peer_snapshot:
            self._render_peers(peers)
            self._last_peer_snapshot = peers

        self.after(1000, self._refresh_peers)

    def _render_peers(self, peers: Dict[str, PeerInfo]) -> None:
        """Rebuild the peer list to mirror the discovery registry."""

        for child in self.peer_scroll.winfo_children():
            child.destroy()

        self._peer_buttons.clear()

        if not peers:
            self.peer_hint = ctk.CTkLabel(
                self.peer_scroll,
                text="Waiting for peers on the LAN...",
                text_color="#9AA4B2",
            )
            self.peer_hint.grid(row=0, column=0, sticky="w", padx=12, pady=12)
            return

        for row, (ip_address, peer) in enumerate(sorted(peers.items()), start=0):
            peer_button = ctk.CTkButton(
                self.peer_scroll,
                text=f"{peer.hostname}\n{peer.ip_address}",
                anchor="w",
                height=54,
                command=lambda ip=ip_address: self._select_peer(ip),
            )
            peer_button.grid(row=row, column=0, sticky="ew", padx=10, pady=8)
            self._peer_buttons[ip_address] = peer_button

    def _select_peer(self, ip_address: str) -> None:
        """Set the active peer used by the transfer action."""

        self.selected_peer_ip = ip_address
        peer = self.discovery.peers.get(ip_address)
        if peer is None:
            self.selection_label.configure(text="No peer selected")
            return

        self.selection_label.configure(text=f"Selected: {peer.hostname} ({peer.ip_address})")

    def _poll_events(self) -> None:
        """Consume transfer events from the background queue."""

        while True:
            try:
                event = self.transfer_manager.event_queue.get_nowait()
            except Empty:
                break

            self._handle_transfer_event(event)

        self.after(150, self._poll_events)

    def _handle_transfer_event(self, event: TransferEvent) -> None:
        """Update the UI from a structured transfer event."""

        if event.event_type == "transfer_complete":
            self._set_progress(1.0, "Verified successfully")
            self._set_status(event.message)
        elif event.event_type == "transfer_error":
            self._set_status(event.message)
            self._set_progress(0.0, "Idle")
        else:
            self._set_status(event.message)

    def _set_status(self, message: str) -> None:
        """Update the status label safely from any thread."""

        self.status_label.configure(text=message)

    def _set_progress(self, value: float, label: str) -> None:
        """Update the progress bar and accompanying label."""

        bounded_value = max(0.0, min(1.0, value))
        self.progress_bar.set(bounded_value)
        self.progress_label.configure(text=label)

    def _format_size(self, size_bytes: int) -> str:
        """Format a byte count into a human-readable string."""

        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_index = 0

        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        return f"{size:.2f} {units[unit_index]}"

    def _on_close(self) -> None:
        """Stop background services and close the window cleanly."""

        try:
            self.discovery.stop()
        finally:
            self.transfer_manager.stop_server()
            self.destroy()


__all__ = ["LANDropUI"]