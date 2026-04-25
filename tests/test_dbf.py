"""Tests for the DBF reader and DBC integration."""

import struct
import pytest
import pandas as pd

from climasus_readdbc._dbf import DBFError, read_dbf_columns, is_dbf
from climasus_readdbc import read_dbc, read_dbf, dbc_to_dbf, DBCError


# ── Synthetic DBF builder ───────────────────────────────────────────────

def _make_dbf(
    fields: list[tuple[str, str, int]],
    records: list[list[str]],
) -> bytes:
    """Build a minimal dBASE III .dbf file in memory.

    Parameters
    ----------
    fields : list of (name, type, length)
    records : list of lists (one inner list per record, matching fields)
    """
    n_fields = len(fields)
    header_size = 32 + n_fields * 32 + 1  # +1 for 0x0D terminator
    record_size = 1 + sum(length for _, _, length in fields)

    # ── Main header (32 bytes) ──
    header = bytearray(32)
    header[0] = 0x03  # dBASE III
    header[1] = 126   # year - 1900 (2026)
    header[2] = 4     # month
    header[3] = 17    # day
    struct.pack_into("<I", header, 4, len(records))
    struct.pack_into("<H", header, 8, header_size)
    struct.pack_into("<H", header, 10, record_size)

    # ── Field descriptors (32 bytes each) ──
    field_descriptors = bytearray()
    for name, ftype, length in fields:
        fd = bytearray(32)
        name_bytes = name.encode("ascii")[:11]
        fd[: len(name_bytes)] = name_bytes
        fd[11] = ord(ftype)
        fd[16] = length
        fd[17] = 0  # decimal count
        field_descriptors.extend(fd)

    # ── Terminator ──
    terminator = b"\r"

    # ── Records ──
    rec_bytes = bytearray()
    for row in records:
        rec_bytes.append(0x20)  # not deleted
        for (_, ftype, length), value in zip(fields, row):
            encoded = value.encode("latin1")[:length]
            if ftype == "N":
                padded = encoded.rjust(length)
            else:
                padded = encoded.ljust(length)
            rec_bytes.extend(padded)

    return bytes(header + field_descriptors + terminator + rec_bytes)


# ── DBF detection ───────────────────────────────────────────────────────

class TestIsDBF:
    def test_valid_dbf(self):
        dbf = _make_dbf([("X", "C", 5)], [["hello"]])
        assert is_dbf(dbf) is True

    def test_too_short(self):
        assert is_dbf(b"\x03" * 10) is False

    def test_wrong_version(self):
        data = bytearray(32)
        data[0] = 0xFF
        assert is_dbf(bytes(data)) is False


# ── DBF reading ─────────────────────────────────────────────────────────

class TestReadDBF:
    @pytest.fixture
    def simple_dbf(self):
        return _make_dbf(
            [("NAME", "C", 10), ("AGE", "N", 3), ("CITY", "C", 15)],
            [
                ["Alice", "25", "São Paulo"],
                ["Bob", "30", "Rio de Janeiro"],
                ["Carol", "28", "Belo Horizon."],
            ],
        )

    def test_column_count(self, simple_dbf):
        cols = read_dbf_columns(simple_dbf)
        assert len(cols) == 3
        assert set(cols.keys()) == {"NAME", "AGE", "CITY"}

    def test_record_count(self, simple_dbf):
        cols = read_dbf_columns(simple_dbf)
        assert len(cols["NAME"]) == 3

    def test_values(self, simple_dbf):
        cols = read_dbf_columns(simple_dbf)
        assert cols["NAME"] == ["Alice", "Bob", "Carol"]
        assert cols["AGE"] == ["25", "30", "28"]

    def test_encoding_latin1(self, simple_dbf):
        cols = read_dbf_columns(simple_dbf, encoding="latin1")
        assert "São Paulo" in cols["CITY"][0]

    def test_single_field(self):
        dbf = _make_dbf([("VALUE", "N", 5)], [["100"], ["200"]])
        cols = read_dbf_columns(dbf)
        assert cols["VALUE"] == ["100", "200"]

    def test_empty_records(self):
        dbf = _make_dbf([("X", "C", 5)], [])
        cols = read_dbf_columns(dbf)
        assert cols["X"] == []

    def test_too_short_raises(self):
        with pytest.raises(DBFError, match="too short"):
            read_dbf_columns(b"\x03" * 10)

    def test_deleted_records_skipped(self):
        """Records flagged as deleted should be skipped by default."""
        dbf_bytes = bytearray(
            _make_dbf(
                [("VAL", "C", 3)],
                [["AAA"], ["BBB"], ["CCC"]],
            )
        )
        # Mark second record as deleted (0x2A)
        header_size = struct.unpack_from("<H", dbf_bytes, 8)[0]
        record_size = struct.unpack_from("<H", dbf_bytes, 10)[0]
        rec2_offset = header_size + record_size  # start of record 2
        dbf_bytes[rec2_offset] = 0x2A
        cols = read_dbf_columns(bytes(dbf_bytes))
        assert len(cols["VAL"]) == 2
        assert cols["VAL"] == ["AAA", "CCC"]

    def test_record_size_zero_raises(self):
        """DBF with record_size=0 must raise, not divide by zero."""
        dbf_bytes = bytearray(_make_dbf([("X", "C", 5)], [["hello"]]))
        struct.pack_into("<H", dbf_bytes, 10, 0)  # force record_size=0
        with pytest.raises(DBFError, match="record_size 0"):
            read_dbf_columns(bytes(dbf_bytes))

    def test_field_length_zero_raises(self):
        """A field with length=0 must raise during parsing."""
        dbf_bytes = bytearray(_make_dbf([("X", "C", 5)], [["hello"]]))
        # Field descriptor starts at byte 32; length byte at offset 16 within it
        dbf_bytes[32 + 16] = 0
        with pytest.raises(DBFError, match="invalid field length 0"):
            read_dbf_columns(bytes(dbf_bytes))

    def test_duplicate_field_names(self):
        """Duplicate field names should get a _N suffix, not overwrite."""
        dbf = _make_dbf(
            [("VAL", "C", 3), ("VAL", "C", 3)],
            [["AAA", "BBB"]],
        )
        cols = read_dbf_columns(dbf)
        assert "VAL" in cols
        assert "VAL_1" in cols
        assert cols["VAL"] == ["AAA"]
        assert cols["VAL_1"] == ["BBB"]


# ── DataFrame API ───────────────────────────────────────────────────────

class TestReadDBFDataFrame:
    def test_returns_dataframe(self):
        dbf = _make_dbf([("X", "C", 5)], [["hello"], ["world"]])
        df = read_dbf(dbf)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["X"]

    def test_read_from_file(self, tmp_path):
        dbf = _make_dbf([("A", "C", 3)], [["abc"]])
        path = tmp_path / "test.dbf"
        path.write_bytes(dbf)
        df = read_dbf(path)
        assert len(df) == 1
        assert df["A"].iloc[0] == "abc"


# ── DBC integration (uses synthetic data) ──────────────────────────────

class TestReadDBC:
    def test_plain_dbf_passthrough(self):
        """If a .dbc file is actually a plain .dbf, it should be read fine."""
        dbf = _make_dbf([("X", "C", 5)], [["hello"]])
        df = read_dbc(dbf)
        assert len(df) == 1
        assert df["X"].iloc[0] == "hello"

    def test_dbc_from_file(self, tmp_path):
        """read_dbc should work with file paths too (plain DBF test)."""
        dbf = _make_dbf([("V", "N", 4)], [["1234"]])
        path = tmp_path / "test.dbc"
        path.write_bytes(dbf)
        df = read_dbc(path)
        assert df["V"].iloc[0] == "1234"

    def test_invalid_dbc_raises(self):
        with pytest.raises(DBCError):
            read_dbc(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")

    def test_dbc_to_dbf_plain(self):
        """dbc_to_dbf should return plain DBF unchanged."""
        dbf = _make_dbf([("A", "C", 3)], [["xyz"]])
        result = dbc_to_dbf(dbf)
        assert result == dbf

    def test_dbc_too_small_raises(self):
        """DBC file smaller than minimum header must raise."""
        with pytest.raises(DBCError, match="too small"):
            dbc_to_dbf(b"\x00" * 13)

    def test_dbc_header_size_zero_raises(self):
        """DBC with header_size=0 must raise."""
        data = bytearray(32)
        data[0] = 0x03
        struct.pack_into("<H", data, 8, 0)  # header_size = 0
        with pytest.raises(DBCError, match="invalid DBC"):
            dbc_to_dbf(bytes(data))

    def test_dbc_header_exceeds_file_raises(self):
        """DBC where header_size+4 >= file length must raise."""
        data = bytearray(64)
        data[0] = 0x03
        struct.pack_into("<I", data, 4, 1000)   # n_records=1000 → not a plain DBF
        struct.pack_into("<H", data, 8, 62)      # header_size = 62, file=64, 62+4>64
        struct.pack_into("<H", data, 10, 100)    # record_size = 100
        with pytest.raises(DBCError, match="invalid DBC"):
            dbc_to_dbf(bytes(data))


# ── Real DBC file test (skipped if no test file available) ──────────────

class TestRealDBC:
    @pytest.fixture
    def dbc_path(self):
        """Look for a real .dbc file in dados/cache or skip."""
        from pathlib import Path

        candidates = [
            Path("dados/cache"),
            Path("climasus4py/dados"),
            Path("."),
        ]
        for base in candidates:
            for dbc in base.rglob("*.dbc"):
                return dbc
        pytest.skip("No real .dbc file found for integration test")

    def test_read_real_dbc(self, dbc_path):
        df = read_dbc(dbc_path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert len(df.columns) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
