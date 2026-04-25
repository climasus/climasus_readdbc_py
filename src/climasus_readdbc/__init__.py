"""climasus_readdbc — Pure-Python reader for DATASUS .dbc files.

Zero external dependencies beyond pandas.  No C compiler needed.

Usage::

    import climasus_readdbc

    df = climasus_readdbc.read_dbc("DOSP2023.dbc")
    df = climasus_readdbc.read_dbf("data.dbf")
    raw = climasus_readdbc.blast_decompress(compressed_bytes)
"""

from __future__ import annotations

import struct
from pathlib import Path

import pandas as pd

from climasus_readdbc._blast import BlastError, blast_decompress
from climasus_readdbc._dbf import DBFError, is_dbf, read_dbf_columns

__version__ = "0.2.0"

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
    """Descomprime um arquivo .dbc para bytes .dbf brutos.

    Args:
        data (bytes): Conteúdo binário de um arquivo .dbc — cabeçalho DBF
            não comprimido seguido dos registros comprimidos via PKWare blast.

    Returns:
        bytes: Dados .dbf descomprimidos, prontos para leitura pelo parser DBF.

    Raises:
        DBCError: Se o arquivo for muito pequeno, tiver ``header_size`` inválido
            ou se a descompressão blast falhar.

    Example:
        >>> with open("DOSP2023.dbc", "rb") as f:
        ...     raw = f.read()
        >>> dbf_bytes = dbc_to_dbf(raw)
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
    """Lê um arquivo .dbc do DATASUS e retorna um pandas DataFrame.

    Args:
        source (str | Path | bytes): Caminho para o arquivo .dbc ou conteúdo
            bruto do arquivo em bytes.
        encoding (str): Codificação de texto para campos de string.
            Padrão: ``"latin1"``.

    Returns:
        pandas.DataFrame: Todos os valores são retornados como strings;
            aplique conversões de tipo downstream (ex.: via ``climasus4py``).

    Raises:
        DBCError: Se o arquivo não for um .dbc ou .dbf válido.
        DBFError: Se o conteúdo DBF descomprimido for inválido.
        OSError: Se o arquivo não puder ser aberto para leitura.

    Example:
        >>> import readdbc
        >>> df = readdbc.read_dbc("DOSP2023.dbc")
        >>> print(df.shape)
        (n_records, n_fields)
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
    """Lê um arquivo .dbf padrão e retorna um pandas DataFrame.

    Args:
        source (str | Path | bytes): Caminho para o arquivo .dbf ou conteúdo
            bruto do arquivo em bytes.
        encoding (str): Codificação de texto para campos de string.
            Padrão: ``"latin1"``.

    Returns:
        pandas.DataFrame: Todos os valores são retornados como strings.

    Raises:
        DBFError: Se o arquivo não for um .dbf válido.
        OSError: Se o arquivo não puder ser aberto para leitura.

    Example:
        >>> import readdbc
        >>> df = readdbc.read_dbf("dados.dbf")
        >>> list(df.columns)[:3]
        ['CAMPO1', 'CAMPO2', 'CAMPO3']
    """
    if isinstance(source, (str, Path)):
        with open(source, "rb") as f:
            raw = f.read()
    else:
        raw = source

    columns = read_dbf_columns(raw, encoding=encoding)
    return pd.DataFrame(columns)
