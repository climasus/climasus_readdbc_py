"""Tests for the blast decompressor.

Uses the canonical test vector from Mark Adler's blast.c:
    Input:  00 04 82 24 25 8f 80 7f
    Output: "AIAIAIAIAIAIA"
"""

import pytest

from readdbc._blast import BlastError, blast_decompress, _construct


# ── Table construction tests ────────────────────────────────────────────

class TestConstruct:
    def test_litlen_expands_to_256(self):
        """Literal code table must expand to 256 symbols."""
        from readdbc._blast import _LITLEN
        count, symbol = _construct(_LITLEN)
        total = sum(count[1:])
        # total coded symbols = len(symbol)
        assert total == len(symbol)
        # count[0] + total should be 256 (count[0] = uncoded symbols)
        assert count[0] + total == 256

    def test_lenlen_expands_to_16(self):
        from readdbc._blast import _LENLEN
        count, symbol = _construct(_LENLEN)
        total = sum(count)
        assert total == 16

    def test_distlen_expands_to_64(self):
        from readdbc._blast import _DISTLEN
        count, symbol = _construct(_DISTLEN)
        total = sum(count)
        assert total == 64


# ── Decompression tests ─────────────────────────────────────────────────

class TestBlastDecompress:
    def test_canonical_vector(self):
        """blast.c reference: 00 04 82 24 25 8f 80 7f → AIAIAIAIAIAIA"""
        compressed = bytes.fromhex("00 04 82 24 25 8f 80 7f".replace(" ", ""))
        result = blast_decompress(compressed)
        assert result == b"AIAIAIAIAIAIA"

    def test_canonical_vector_length(self):
        compressed = bytes.fromhex("0004822425 8f807f".replace(" ", ""))
        result = blast_decompress(compressed)
        assert len(result) == 13  # "AIAIAIAIAIAIA" = 13 chars

    def test_empty_input_raises(self):
        with pytest.raises(BlastError):
            blast_decompress(b"")

    def test_truncated_input_raises(self):
        with pytest.raises(BlastError):
            blast_decompress(b"\x00")  # just the lit byte, no dict byte

    def test_invalid_lit_flag_raises(self):
        with pytest.raises(BlastError, match="invalid literal flag"):
            blast_decompress(b"\x02\x04")

    def test_invalid_dict_raises(self):
        with pytest.raises(BlastError, match="invalid dictionary bits"):
            blast_decompress(b"\x00\x03")  # dict=3 is invalid

    def test_result_is_bytes(self):
        compressed = bytes.fromhex("00048224258f807f")
        result = blast_decompress(compressed)
        assert isinstance(result, bytes)

    def test_memoryview_input(self):
        """Should accept memoryview as well as bytes."""
        compressed = bytes.fromhex("00048224258f807f")
        mv = memoryview(compressed)
        result = blast_decompress(mv)
        assert result == b"AIAIAIAIAIAIA"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
