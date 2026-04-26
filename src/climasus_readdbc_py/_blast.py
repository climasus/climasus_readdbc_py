"""Compatibility wrapper for ``climasus_readdbc._blast``."""

from __future__ import annotations

from climasus_readdbc._blast import (
    _DISTLEN,
    _LENLEN,
    _LITLEN,
    BlastError,
    _construct,
    blast_decompress,
)

__all__ = [
    "BlastError",
    "blast_decompress",
    "_construct",
    "_LITLEN",
    "_LENLEN",
    "_DISTLEN",
]
