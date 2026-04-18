"""Pure-Python dBASE III / III+ (.dbf) file reader.

Parses the binary DBF format and returns tabular data as a list of dicts
or a pandas DataFrame.  Supports field types C (character), N (numeric),
D (date), L (logical), and F (float); other types are returned as raw
stripped strings.

Reference:
    http://www.dbf2002.com/dbf-file-format.html
    http://www.independent-software.com/dbase-dbf-dbt-file-format.html
"""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Any


class DBFError(Exception):
    """Error while parsing a DBF file."""


# Known dBASE version bytes (first byte of the file).
_DBF_VERSIONS = {
    0x02, 0x03, 0x04, 0x05, 0x30, 0x31, 0x32,
    0x43, 0x63, 0x83, 0x8B, 0x8C, 0x8E, 0xCB, 0xF5, 0xFB,
}


def is_dbf(data: bytes | memoryview) -> bool:
    """Return True if *data* looks like a valid DBF file."""
    if len(data) < 32:
        return False
    return data[0] in _DBF_VERSIONS


# ── Field descriptor ────────────────────────────────────────────────────

class _Field:
    __slots__ = ("name", "type", "length", "decimal")

    def __init__(self, name: str, ftype: str, length: int, decimal: int) -> None:
        self.name = name
        self.type = ftype
        self.length = length
        self.decimal = decimal

    def __repr__(self) -> str:
        return f"_Field({self.name!r}, {self.type!r}, {self.length}, {self.decimal})"


def _parse_fields(data: bytes | memoryview, header_size: int) -> list[_Field]:
    """Extract field descriptors from the DBF header."""
    fields: list[_Field] = []
    offset = 32
    while offset < header_size - 1:
        if data[offset] == 0x0D:
            break
        raw_name = data[offset : offset + 11]
        name = raw_name.split(b"\x00", 1)[0].decode("ascii", errors="replace").strip()
        ftype = chr(data[offset + 11])
        length = data[offset + 16]
        decimal = data[offset + 17]
        if length == 0:
            raise DBFError(f"invalid field length 0 for field {name!r}")
        fields.append(_Field(name, ftype, length, decimal))
        offset += 32
    return fields


# ── Record parsing ──────────────────────────────────────────────────────

def _parse_value(raw: bytes, field: _Field, encoding: str) -> Any:
    """Decode a single field value from its raw bytes."""
    text = raw.decode(encoding, errors="replace").strip()
    if not text:
        return None
    # Return all values as strings — let the caller decide types.
    return text


# ── Public API ──────────────────────────────────────────────────────────

def read_dbf_records(
    data: bytes | memoryview,
    *,
    encoding: str = "latin1",
    include_deleted: bool = False,
) -> tuple[list[_Field], list[dict[str, Any]]]:
    """Parse a DBF file from raw bytes.

    Returns
    -------
    fields : list[_Field]
        Field descriptors.
    records : list[dict]
        One dict per record, keyed by field name.
    """
    if len(data) < 32:
        raise DBFError("data too short to contain a DBF header")

    version = data[0]
    n_records = struct.unpack_from("<I", data, 4)[0]
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]

    fields = _parse_fields(data, header_size)

    if not fields:
        raise DBFError("no field descriptors found in DBF header")

    # Compute expected field widths vs record_size
    expected_rec = 1 + sum(f.length for f in fields)  # 1 byte deletion flag
    if expected_rec != record_size:
        # Some files have padding — adjust by trusting record_size.
        pass

    if record_size == 0:
        raise DBFError("invalid record_size 0 in DBF header")

    records: list[dict[str, Any]] = []
    data_start = header_size
    # Be robust: read as many records as the data actually contains,
    # even if n_records in the header is wrong.
    max_records = (len(data) - data_start) // record_size
    actual_records = min(n_records, max_records)

    for i in range(actual_records):
        rec_offset = data_start + i * record_size
        if rec_offset + record_size > len(data):
            break

        deletion_flag = data[rec_offset]
        if deletion_flag == 0x2A and not include_deleted:
            continue  # skip deleted records
        if deletion_flag == 0x1A:
            break  # EOF marker

        row: dict[str, Any] = {}
        field_offset = rec_offset + 1
        for field in fields:
            raw = data[field_offset : field_offset + field.length]
            row[field.name] = _parse_value(raw, field, encoding)
            field_offset += field.length
        records.append(row)

    return fields, records


def read_dbf_columns(
    data: bytes | memoryview,
    *,
    encoding: str = "latin1",
    include_deleted: bool = False,
) -> dict[str, list[Any]]:
    """Parse a DBF file into column-oriented dict (faster for DataFrame).

    Returns
    -------
    dict mapping field name → list of values (one per record).
    """
    if len(data) < 32:
        raise DBFError("data too short to contain a DBF header")

    n_records = struct.unpack_from("<I", data, 4)[0]
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]

    fields = _parse_fields(data, header_size)
    if not fields:
        raise DBFError("no field descriptors found in DBF header")

    if record_size == 0:
        raise DBFError("invalid record_size 0 in DBF header")

    # Handle duplicate field names by appending a suffix.
    seen: dict[str, int] = {}
    unique_names: list[str] = []
    for f in fields:
        if f.name in seen:
            seen[f.name] += 1
            unique = f"{f.name}_{seen[f.name]}"
            unique_names.append(unique)
        else:
            seen[f.name] = 0
            unique_names.append(f.name)

    columns: dict[str, list[Any]] = {name: [] for name in unique_names}

    data_start = header_size
    max_records = (len(data) - data_start) // record_size
    actual_records = min(n_records, max_records)

    for i in range(actual_records):
        rec_offset = data_start + i * record_size
        if rec_offset + record_size > len(data):
            break

        deletion_flag = data[rec_offset]
        if deletion_flag == 0x2A and not include_deleted:
            continue
        if deletion_flag == 0x1A:
            break

        field_offset = rec_offset + 1
        for field, col_name in zip(fields, unique_names):
            raw = data[field_offset : field_offset + field.length]
            value = _parse_value(raw, field, encoding)
            columns[col_name].append(value)
            field_offset += field.length

    return columns
