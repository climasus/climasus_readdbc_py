# climasus_readdbc_py

Leitor puro-Python para arquivos `.dbc` do DATASUS — formato dBASE III com compressão PKWARE blast. Sem dependências C, sem compilador necessário.

[![PyPI version](https://img.shields.io/pypi/v/climasus_readdbc_py.svg)](https://pypi.org/project/climasus_readdbc_py/)
[![Python Versions](https://img.shields.io/pypi/pyversions/climasus_readdbc_py.svg)](https://pypi.org/project/climasus_readdbc_py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

## Instalação

```bash
pip install climasus_readdbc_py
```

## Uso rápido

```python
import climasus_readdbc_py

# Ler arquivo .dbc diretamente como DataFrame pandas
df = climasus_readdbc_py.read_dbc("DOSP2023.dbc")

# Ler arquivo .dbf
df = climasus_readdbc_py.read_dbf("dados.dbf")
```

## Links

- [GitHub](https://github.com/climasus/climasus_readdbc_py)
- [PyPI](https://pypi.org/project/climasus_readdbc_py/)
- [Ecossistema CLIMA-SUS](https://github.com/climasus)
