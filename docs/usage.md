# Uso

## Ler arquivo `.dbc`

```python
import climasus_readdbc_py

df = climasus_readdbc_py.read_dbc("DOSP2023.dbc")
print(df.shape)
print(df.head())
```

## Ler arquivo `.dbf`

```python
df = climasus_readdbc_py.read_dbf("dados.dbf")
```

## Converter bytes `.dbc` para bytes `.dbf`

```python
from pathlib import Path

raw = Path("DOSP2023.dbc").read_bytes()
dbf_bytes = climasus_readdbc_py.dbc_to_dbf(raw)
```

## Encoding

Por padrão, strings são decodificadas em `latin-1` (padrão DATASUS).
Para sobrescrever:

```python
df = climasus_readdbc_py.read_dbc("arquivo.dbc", encoding="utf-8")
```
