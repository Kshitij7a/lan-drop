"""Chunking and reconstruction helpers for LAN-Drop.

The file processor is intentionally separate from the transfer engine so the
network layer can operate on immutable chunk metadata and keep disk I/O in one
place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import hashlib
import shutil
import tempfile
from typing import List, Sequence

from core.merkle import MerkleTree


CHUNK_SIZE_BYTES = 1024 * 1024


def _sha256_hex(data: bytes) -> str:
    """Return the SHA-256 digest for *data* as a hexadecimal string."""

    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class FileChunk:
    """Metadata for one chunk produced from an input file."""

    index: int
    offset: int
    size: int
    sha256: str
    temp_path: Path


@dataclass
class ChunkManifest:
    """Describes a file that has been split into temporary chunks."""

    source_path: Path
    temp_dir: Path
    file_size: int
    chunk_size: int = CHUNK_SIZE_BYTES
    chunks: List[FileChunk] = field(default_factory=list)

    @property
    def total_chunks(self) -> int:
        """Return the number of chunks in this manifest."""

        return len(self.chunks)


class FileProcessor:
    """Split, hash, reconstruct, and clean up file chunks."""

    def __init__(self, chunk_size: int = CHUNK_SIZE_BYTES) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        self.chunk_size = chunk_size

    def split_file(
        self,
        file_path: str | Path,
        temp_root: str | Path | None = None,
    ) -> ChunkManifest:
        """Split a file into temporary chunks and return a manifest.

        Each chunk is written to disk immediately to keep memory usage low for
        very large files.
        """

        source_path = Path(file_path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"File not found: {source_path}")

        if temp_root is None:
            temp_dir = Path(tempfile.mkdtemp(prefix=f"lan_drop_{source_path.stem}_"))
        else:
            temp_dir = Path(temp_root).expanduser().resolve()
            temp_dir.mkdir(parents=True, exist_ok=True)

        manifest = ChunkManifest(
            source_path=source_path,
            temp_dir=temp_dir,
            file_size=source_path.stat().st_size,
            chunk_size=self.chunk_size,
        )

        with source_path.open("rb") as source_file:
            offset = 0
            index = 0

            while True:
                chunk_data = source_file.read(self.chunk_size)
                if not chunk_data:
                    break

                chunk_path = temp_dir / f"chunk_{index:08d}.part"
                chunk_path.write_bytes(chunk_data)

                manifest.chunks.append(
                    FileChunk(
                        index=index,
                        offset=offset,
                        size=len(chunk_data),
                        sha256=_sha256_hex(chunk_data),
                        temp_path=chunk_path,
                    )
                )

                offset += len(chunk_data)
                index += 1

        return manifest

    def load_chunk(self, chunk: FileChunk) -> bytes:
        """Load a chunk from its temporary file location."""

        if not chunk.temp_path.is_file():
            raise FileNotFoundError(f"Chunk file missing: {chunk.temp_path}")

        return chunk.temp_path.read_bytes()

    def build_merkle_tree(self, manifest: ChunkManifest) -> MerkleTree:
        """Build a Merkle tree from a chunk manifest."""

        if not manifest.chunks:
            raise ValueError("Cannot build a Merkle tree from an empty manifest")

        chunk_data = [self.load_chunk(chunk) for chunk in manifest.chunks]
        return MerkleTree(chunk_data)

    def reconstruct_file(
        self,
        chunks: Sequence[bytes | FileChunk],
        output_path: str | Path,
    ) -> Path:
        """Rebuild a file from chunk data or chunk descriptors."""

        destination = Path(output_path).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)

        with destination.open("wb") as output_file:
            for chunk in chunks:
                if isinstance(chunk, FileChunk):
                    chunk_data = self.load_chunk(chunk)
                else:
                    chunk_data = chunk
                output_file.write(chunk_data)

        return destination

    def cleanup_manifest(self, manifest: ChunkManifest) -> None:
        """Remove the temporary directory created for a manifest."""

        if manifest.temp_dir.exists():
            shutil.rmtree(manifest.temp_dir, ignore_errors=True)

    def verify_chunk_hash(self, chunk_data: bytes, expected_hash: str) -> bool:
        """Compare a chunk against an expected SHA-256 digest."""

        return _sha256_hex(chunk_data) == expected_hash


__all__ = [
    "CHUNK_SIZE_BYTES",
    "ChunkManifest",
    "FileChunk",
    "FileProcessor",
]