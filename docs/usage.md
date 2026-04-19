# Uso

## Ler arquivo `.dbc`

```python
import readdbc

df = readdbc.read_dbc("DOSP2023.dbc")
print(df.shape)
print(df.head())
```

## Ler arquivo `.dbf`

```python
df = readdbc.read_dbf("dados.dbf")
```

## Converter `.dbc` para `.dbf` sem carregar na memória

```python
readdbc.dbc_to_dbf("DOSP2023.dbc", "DOSP2023.dbf")
```

## Encoding

Por padrão, strings são decodificadas em `latin-1` (padrão DATASUS).
Para sobrescrever:

```python
df = readdbc.read_dbc("arquivo.dbc", encoding="utf-8")
```
