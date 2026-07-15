"""Merkle tree implementation for chunk-level file integrity checks.

This module keeps the tree logic self-contained so the transfer layer can
request or validate individual chunks without rebuilding the whole file.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import List, Sequence


def _sha256_hex(data: bytes) -> str:
    """Return the SHA-256 digest for *data* as a hexadecimal string."""

    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class MerkleProofStep:
    """One step in a Merkle inclusion proof.

    Attributes:
        sibling_hash: Hex digest of the adjacent node.
        sibling_position: Either "left" or "right" relative to the current
            node when the proof is reconstructed.
    """

    sibling_hash: str
    sibling_position: str


class MerkleTree:
    """Build and validate a SHA-256 Merkle tree over file chunks.

    The tree stores each level from leaves to root. When a level contains an
    odd number of nodes, the last node is duplicated, which is the standard
    Merkle-tree approach used by many content-addressed systems.
    """

    def __init__(self, chunks: Sequence[bytes]) -> None:
        if not chunks:
            raise ValueError("MerkleTree requires at least one chunk")

        self._chunk_hashes: List[str] = [_sha256_hex(chunk) for chunk in chunks]
        self._levels: List[List[str]] = [self._chunk_hashes.copy()]
        self._build_tree()

    @classmethod
    def from_chunk_hashes(cls, chunk_hashes: Sequence[str]) -> "MerkleTree":
        """Construct a tree directly from precomputed leaf hashes."""

        if not chunk_hashes:
            raise ValueError("MerkleTree requires at least one chunk hash")

        tree = cls.__new__(cls)
        tree._chunk_hashes = list(chunk_hashes)
        tree._levels = [tree._chunk_hashes.copy()]
        tree._build_tree()
        return tree

    @property
    def chunk_hashes(self) -> List[str]:
        """Return a copy of the leaf hashes in chunk order."""

        return self._chunk_hashes.copy()

    @property
    def levels(self) -> List[List[str]]:
        """Return a defensive copy of the full tree structure."""

        return [level.copy() for level in self._levels]

    @property
    def root_hash(self) -> str:
        """Return the root hash representing the whole file."""

        return self._levels[-1][0]

    def get_proof(self, chunk_index: int) -> List[MerkleProofStep]:
        """Return an inclusion proof for the chunk at *chunk_index*.

        The proof can be used to verify one chunk against the tree root without
        rehashing every other chunk.
        """

        self._validate_chunk_index(chunk_index)

        proof: List[MerkleProofStep] = []
        index = chunk_index

        for level in self._levels[:-1]:
            sibling_index = index ^ 1
            if sibling_index >= len(level):
                sibling_index = index

            sibling_position = "left" if sibling_index < index else "right"
            proof.append(
                MerkleProofStep(
                    sibling_hash=level[sibling_index],
                    sibling_position=sibling_position,
                )
            )
            index //= 2

        return proof

    def verify_chunk(
        self,
        chunk_data: bytes,
        chunk_index: int,
        proof: Sequence[MerkleProofStep] | None = None,
    ) -> bool:
        """Verify a chunk against the tree root.

        Args:
            chunk_data: Raw chunk bytes to validate.
            chunk_index: Zero-based chunk index in the original file.
            proof: Optional inclusion proof from :meth:`get_proof`. When not
                provided, the proof is generated from the tree.

        Returns:
            True if the chunk is valid for this tree; otherwise False.
        """

        self._validate_chunk_index(chunk_index)

        if proof is None:
            proof = self.get_proof(chunk_index)

        current_hash = _sha256_hex(chunk_data)

        for step in proof:
            if step.sibling_position == "left":
                current_hash = _sha256_hex(
                    bytes.fromhex(step.sibling_hash) + bytes.fromhex(current_hash)
                )
            else:
                current_hash = _sha256_hex(
                    bytes.fromhex(current_hash) + bytes.fromhex(step.sibling_hash)
                )

        return current_hash == self.root_hash

    def verify_chunk_hash(self, chunk_hash: str, chunk_index: int) -> bool:
        """Verify a precomputed chunk hash against the stored leaf hash."""

        self._validate_chunk_index(chunk_index)
        return self._chunk_hashes[chunk_index] == chunk_hash

    def _build_tree(self) -> None:
        """Build the internal level representation from the leaf hashes."""

        current_level = self._levels[0]

        while len(current_level) > 1:
            next_level: List[str] = []

            for index in range(0, len(current_level), 2):
                left = current_level[index]
                right = current_level[index + 1] if index + 1 < len(current_level) else left
                next_level.append(_sha256_hex(bytes.fromhex(left) + bytes.fromhex(right)))

            self._levels.append(next_level)
            current_level = next_level

    def _validate_chunk_index(self, chunk_index: int) -> None:
        """Ensure the provided chunk index is valid for this tree."""

        if chunk_index < 0 or chunk_index >= len(self._chunk_hashes):
            raise IndexError("chunk_index out of range")


__all__ = ["MerkleProofStep", "MerkleTree"]