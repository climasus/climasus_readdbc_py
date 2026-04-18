"""Pure-Python PKWare DCL Implode (blast) decompressor.

Port of Mark Adler's blast.c v1.3 (zlib/contrib/blast, 24 Aug 2013).

The PKWare Compression Library uses a variant of LZ77 with Shannon-Fano
coding.  DATASUS .dbc files use this compression over standard dBASE III
(.dbf) data.  This module provides the decompressor ("blast" / "explode").

Reference:
    Mark Adler, blast.c — https://github.com/madler/zlib/blob/master/contrib/blast/blast.c
    Ben Rudiak-Gould, format description on comp.compression (2001-08-13)
"""

from __future__ import annotations

_MAXBITS = 13
_MAXWIN = 4096

# ── Packed Huffman code lengths (from blast.c) ──────────────────────────
# Each byte encodes (count, bit-length): count = (byte >> 4) + 1,
# length = byte & 0x0F.  The construct() function expands these into
# the full per-symbol length arrays.

# Literal codes (256 symbols)
_LITLEN = bytes([
    11, 124, 8, 7, 28, 7, 188, 13, 76, 4, 10, 8, 12, 10, 12, 10, 8, 23, 8,
    9, 7, 6, 7, 8, 7, 6, 55, 8, 23, 24, 12, 11, 7, 9, 11, 12, 6, 7, 22, 5,
    7, 24, 6, 11, 9, 6, 7, 22, 7, 11, 38, 7, 9, 8, 25, 11, 8, 11, 9, 12,
    8, 12, 5, 38, 5, 38, 5, 11, 7, 5, 6, 21, 6, 10, 53, 8, 7, 24, 10, 27,
    44, 253, 253, 253, 252, 252, 252, 13, 12, 45, 12, 45, 12, 61, 12, 45,
    44, 173,
])

# Length codes (16 symbols)
_LENLEN = bytes([2, 35, 36, 53, 38, 23])

# Distance codes (64 symbols)
_DISTLEN = bytes([2, 20, 53, 230, 247, 151, 248])

# Base values for length codes 0..15
_BASE = (3, 2, 4, 5, 6, 7, 8, 9, 10, 12, 16, 24, 40, 72, 136, 264)

# Extra bits for length codes 0..15
_EXTRA = (0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8)


class BlastError(Exception):
    """Error during blast decompression."""


# ── Huffman table construction ──────────────────────────────────────────

def _construct(rep: bytes) -> tuple[list[int], list[int]]:
    """Build canonical Huffman decode tables from packed representation.

    Returns
    -------
    count : list[int]
        count[i] = number of symbols with code length *i* (0 .. MAXBITS).
    symbol : list[int]
        Symbols sorted by code length, then by original order.
    """
    # Expand packed (count, length) pairs into per-symbol lengths.
    lengths: list[int] = []
    for b in rep:
        cnt = (b >> 4) + 1
        bit_len = b & 0x0F
        lengths.extend([bit_len] * cnt)
    n = len(lengths)

    # Count symbols of each length.
    count = [0] * (_MAXBITS + 1)
    for bl in lengths:
        count[bl] += 1

    if count[0] == n:
        # All lengths zero — empty code.
        return count, []

    # Verify the code is not over-subscribed.
    left = 1
    for bl in range(1, _MAXBITS + 1):
        left <<= 1
        left -= count[bl]
        if left < 0:
            raise BlastError("over-subscribed Huffman code")

    # Generate offsets into symbol table for each length.
    offs = [0] * (_MAXBITS + 1)
    for bl in range(1, _MAXBITS):
        offs[bl + 1] = offs[bl] + count[bl]

    # Sort symbols by length, preserving order within each length.
    total_coded = sum(count[1:])
    symbol = [0] * total_coded
    for sym in range(n):
        if lengths[sym] != 0:
            symbol[offs[lengths[sym]]] = sym
            offs[lengths[sym]] += 1

    return count, symbol


# ── Pre-built static tables (built once at import time) ─────────────────

_lit_count, _lit_symbol = _construct(_LITLEN)
_len_count, _len_symbol = _construct(_LENLEN)
_dist_count, _dist_symbol = _construct(_DISTLEN)


# ── Bit reader ──────────────────────────────────────────────────────────

class _BitReader:
    """Read bits from a bytes buffer, LSB first."""

    __slots__ = ("_data", "_pos", "_bitbuf", "_bitcnt")

    def __init__(self, data: bytes | memoryview) -> None:
        self._data = data
        self._pos = 0
        self._bitbuf = 0
        self._bitcnt = 0

    def bits(self, need: int) -> int:
        """Return *need* bits from the input stream."""
        val = self._bitbuf
        while self._bitcnt < need:
            if self._pos >= len(self._data):
                raise BlastError("unexpected end of compressed input")
            val |= self._data[self._pos] << self._bitcnt
            self._pos += 1
            self._bitcnt += 8
        self._bitbuf = val >> need
        self._bitcnt -= need
        return val & ((1 << need) - 1)

    def decode(self, count: list[int], symbol: list[int]) -> int:
        """Decode one Huffman symbol using *count*/*symbol* tables.

        Codes are stored bit-reversed in the stream; this function inverts
        each bit to allow simple integer comparisons during canonical
        decoding (see blast.c ``decode()``).
        """
        bitbuf = self._bitbuf
        left = self._bitcnt
        code = 0
        first = 0
        index = 0
        length = 1
        next_idx = 1  # start at count[1]

        while True:
            while left > 0:
                left -= 1
                code |= (bitbuf & 1) ^ 1  # invert bit
                bitbuf >>= 1
                cnt = count[next_idx]
                next_idx += 1
                if code < first + cnt:
                    self._bitbuf = bitbuf
                    self._bitcnt = (self._bitcnt - length) & 7
                    return symbol[index + (code - first)]
                index += cnt
                first += cnt
                first <<= 1
                code <<= 1
                length += 1

            left = (_MAXBITS + 1) - length
            if left == 0:
                break
            if self._pos >= len(self._data):
                raise BlastError("unexpected end of compressed input")
            bitbuf = self._data[self._pos]
            self._pos += 1
            if left > 8:
                left = 8

        raise BlastError("incomplete Huffman code")


# ── Public API ──────────────────────────────────────────────────────────

def blast_decompress(data: bytes | memoryview) -> bytes:
    """Decompress a PKWare DCL Implode (blast) compressed stream.

    Parameters
    ----------
    data : bytes
        Raw compressed payload (without any DBC header — just the blast
        stream starting with the *lit* flag byte).

    Returns
    -------
    bytes
        Decompressed data.

    Raises
    ------
    BlastError
        If the input is malformed.
    """
    reader = _BitReader(data)
    output = bytearray()
    window = bytearray(_MAXWIN)
    wnext = 0
    first = True  # True until the window has been filled at least once

    # ── Stream header ───────────────────────────────────────────────────
    lit = reader.bits(8)
    if lit > 1:
        raise BlastError(f"invalid literal flag: {lit}")
    dict_bits = reader.bits(8)
    if dict_bits < 4 or dict_bits > 6:
        raise BlastError(f"invalid dictionary bits: {dict_bits}")

    # ── Decode literals and length/distance pairs ───────────────────────
    while True:
        if reader.bits(1):
            # ── Length / distance pair ──────────────────────────────────
            sym = reader.decode(_len_count, _len_symbol)
            length = _BASE[sym] + reader.bits(_EXTRA[sym])
            if length == 519:
                break  # end-of-stream code

            # Distance extra bits depend on length (2 → always 2, else dict).
            dist_extra = 2 if length == 2 else dict_bits
            dist = reader.decode(_dist_count, _dist_symbol) << dist_extra
            dist += reader.bits(dist_extra)
            dist += 1

            if first and dist > wnext:
                raise BlastError("distance too far back")

            # Copy *length* bytes from *dist* bytes back in the window.
            for _ in range(length):
                src = (wnext - dist) % _MAXWIN
                b = window[src]
                window[wnext] = b
                output.append(b)
                wnext += 1
                if wnext == _MAXWIN:
                    wnext = 0
                    first = False
        else:
            # ── Literal byte ───────────────────────────────────────────
            sym = reader.decode(_lit_count, _lit_symbol) if lit else reader.bits(8)
            window[wnext] = sym
            output.append(sym)
            wnext += 1
            if wnext == _MAXWIN:
                wnext = 0
                first = False

    return bytes(output)
