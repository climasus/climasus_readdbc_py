"""readdbc — Pure-Python reader for DATASUS .dbc files.

Zero external dependencies beyond pandas.  No C compiler needed.

Usage::

    import readdbc

    df = readdbc.read_dbc("DOSP2023.dbc")
    df = readdbc.read_dbf("data.dbf")
    raw = readdbc.blast_decompress(compressed_bytes)
"""

from __future__ import annotations

import struct
from pathlib import Path

import pandas as pd

from readdbc._blast import BlastError, blast_decompress
from readdbc._dbf import DBFError, is_dbf, read_dbf_columns

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "read_dbc",
    "read_dbf",
    "blast_decompress",
    "dbc_to_dbf",
    "BlastError",
    "DBFError",
    "DBCError",
]


class DBCError(Exception):
    """Error while reading a .dbc file."""


# ── DBC format ──────────────────────────────────────────────────────────
#
# Reference: danicat/read.dbc  src/dbc2dbf.c  (v1.2.0, Nov 2025)
#            DBC_FORMAT.md / INTERNALS.md
#
# Layout:
#   [0 .. H)       Uncompressed DBF header (H = uint16 LE at bytes 8-9)
#   [H .. H+4)     4-byte CRC / padding  (skipped)
#   [H+4 .. EOF)   PKWare DCL Implode (blast) compressed record data
#
# Decompression produces: dbf_header + decompressed_records = complete DBF.


_CRC_SIZE = 4  # padding / CRC32 between header and compressed stream


def _dbf_expected_size(data: bytes) -> int:
    """Return expected uncompressed DBF file size from its header fields."""
    if len(data) < 12:
        return 0
    n_records = struct.unpack_from("<I", data, 4)[0]
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]
    return header_size + n_records * record_size


def _is_plain_dbf(data: bytes) -> bool:
    """Return True if *data* is a complete, uncompressed DBF file."""
    if not is_dbf(data):
        return False
    expected = _dbf_expected_size(data)
    # Allow tolerance for trailing EOF marker (0x1A) or minor padding.
    return expected > 0 and abs(len(data) - expected) <= 512


def dbc_to_dbf(data: bytes) -> bytes:
    """Decompress a .dbc file to raw .dbf bytes.

    Parameters
    ----------
    data : bytes
        Contents of a .dbc file.

    Returns
    -------
    bytes
        Decompressed .dbf data.
    """
    # ── Plain DBF pass-through ──────────────────────────────────────────
    if _is_plain_dbf(data):
        return data

    # ── DBC decompression ───────────────────────────────────────────────
    if len(data) < 14:
        raise DBCError("file too small to be a valid DBC")

    # H = DBF header size, stored at bytes 8-9 (standard DBF header_size field)
    header_size = struct.unpack_from("<H", data, 8)[0]

    if header_size < 32 or header_size + _CRC_SIZE >= len(data):
        raise DBCError(
            f"invalid DBC: header_size={header_size} but file is {len(data)} bytes"
        )

    # Uncompressed DBF header (first H bytes of the file, verbatim).
    dbf_header = data[:header_size]

    # Compressed record data starts after the header + 4-byte CRC/padding.
    compressed_offset = header_size + _CRC_SIZE
    compressed = data[compressed_offset:]

    try:
        record_data = blast_decompress(compressed)
    except BlastError as e:
        raise DBCError(f"blast decompression failed: {e}") from e

    return bytes(dbf_header) + record_data


# ── Public API ──────────────────────────────────────────────────────────

def read_dbc(
    source: str | Path | bytes,
    *,
    encoding: str = "latin1",
) -> pd.DataFrame:
    """Read a DATASUS .dbc file and return a pandas DataFrame.

    Parameters
    ----------
    source : str, Path, or bytes
        File path or raw file contents.
    encoding : str
        Text encoding for string fields (default ``latin1``).

    Returns
    -------
    pandas.DataFrame
        All values are returned as strings; apply type conversions
        downstream (e.g. via ``climasus4py``).
    """
    if isinstance(source, (str, Path)):
        with open(source, "rb") as f:
            raw = f.read()
    else:
        raw = source

    dbf_data = dbc_to_dbf(raw)
    columns = read_dbf_columns(dbf_data, encoding=encoding)
    return pd.DataFrame(columns)


def read_dbf(
    source: str | Path | bytes,
    *,
    encoding: str = "latin1",
) -> pd.DataFrame:
    """Read a standard .dbf file and return a pandas DataFrame.

    Parameters
    ----------
    source : str, Path, or bytes
        File path or raw file contents.
    encoding : str
        Text encoding for string fields (default ``latin1``).
    """
    if isinstance(source, (str, Path)):
        with open(source, "rb") as f:
            raw = f.read()
    else:
        raw = source

    columns = read_dbf_columns(raw, encoding=encoding)
    return pd.DataFrame(columns)
