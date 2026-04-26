# climasus_readdbc_py

[![PyPI version](https://img.shields.io/pypi/v/climasus_readdbc_py.svg)](https://pypi.org/project/climasus_readdbc_py/)
[![CI](https://github.com/climasus/climasus_readdbc_py/actions/workflows/ci.yml/badge.svg)](https://github.com/climasus/climasus_readdbc_py/actions/workflows/ci.yml)
[![Python Versions](https://img.shields.io/pypi/pyversions/climasus_readdbc_py.svg)](https://pypi.org/project/climasus_readdbc_py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Leitor puro-Python para arquivos `.dbc` do DATASUS — formato dBASE III com compressão PKWARE blast. Sem dependências C, sem compilador necessário.

---

## Instalação

```bash
pip install climasus_readdbc_py
```

**Requisitos:** Python ≥ 3.10, pandas ≥ 2.0

---

## Uso rápido

```python
import climasus_readdbc_py

# Ler arquivo .dbc diretamente como DataFrame pandas
df = climasus_readdbc_py.read_dbc("DOSP2023.dbc")

# Ler arquivo .dbf comum
df = climasus_readdbc_py.read_dbf("dados.dbf")

# Descomprimir bytes brutos de um .dbc para .dbf
with open("DOSP2023.dbc", "rb") as f:
    compressed = f.read()

raw_dbf_bytes = climasus_readdbc_py.blast_decompress(compressed)
```

`climasus_readdbc` continua disponível como alias legado para compatibilidade,
mas código novo deve usar `climasus_readdbc_py`.

---

## O formato DBC

Os arquivos `.dbc` são distribuídos pelo [DATASUS](https://datasus.saude.gov.br/) para transferência eficiente dos microdados do SUS (Sistema Único de Saúde). Internamente, são arquivos dBASE III (`.dbf`) cujo bloco de registros foi comprimido com o algoritmo **PKWARE DCL Implode** (também chamado de *blast*).

**Layout interno:**

```
[0 .. H)       Cabeçalho DBF não comprimido (H = uint16 LE nos bytes 8-9)
[H .. H+4)     4 bytes de CRC / padding (ignorados)
[H+4 .. EOF)   Dados comprimidos com PKWare blast
```

A descompressão produz: `cabeçalho_dbf + registros_descomprimidos = DBF completo`.

---

## API de referência

| Função / Classe       | Descrição                                              |
|-----------------------|--------------------------------------------------------|
| `read_dbc(path)`      | Lê um `.dbc` e retorna `pd.DataFrame`                  |
| `read_dbf(path)`      | Lê um `.dbf` e retorna `pd.DataFrame`                  |
| `blast_decompress(b)` | Descomprime bytes brutos do formato blast              |
| `dbc_to_dbf(b)`       | Converte bytes `.dbc` para bytes `.dbf`                |
| `BlastError`          | Exceção para erros de descompressão                    |
| `DBFError`            | Exceção para erros de leitura DBF                      |
| `DBCError`            | Exceção para erros de leitura DBC                      |

---

## Projeto ClimaSUS

Este pacote faz parte do ecossistema **ClimaSUS** — plataforma de análise de dados climáticos e de saúde pública do SUS.

- Repositório principal: <https://github.com/climasus/climasus_readdbc_py>
- Organização: <https://github.com/climasus>
