# DuckDB HTTP SQLAlchemy Plugin

A lightweight SQLAlchemy dialect/plugin for connecting to **DuckDB**, on top of httpserver duckdb extension. Works with **Superset** for visualization.

## Features

- Connect to DuckDB via SQLAlchemy.
- Support multiple connection read/write.
- Support auth:
  - Basic auth : duckdb_http://alice:secret@localhost:9999
  - API key: duckdb_http://localhost:9999?api_key=SECRETKEY

---

## Installation

```bash
# Install DuckDB and SQLAlchemy
pip install duckdb sqlalchemy
```

Then, install this plugin (if packaged locally):

```bash
pip install duckdb_http
```

---

## Usage

### 1. Connect to DuckDB

```python
from sqlalchemy import create_engine, text

# Connect to local DuckDB database
engine = create_engine("duckdb_http://user:pass@localhost:9999")
# engine = create_engine("duckdb_http://localhost:9999?api_key=secretkey")

with engine.connect() as conn:
    # Execute queries using SQLAlchemy text()
    result = conn.execute(text("SELECT 1"))
    print(result.fetchone())
```

---

## Notes

- Always wrap raw SQL with `text()` when using SQLAlchemy 2.x.
- Compatible with Superset: once installed, you can select **DuckDB** as a database backend.
- Supports multiple schemas and introspection.
