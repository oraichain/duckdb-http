from sqlalchemy.dialects import registry

registry.register("duckdb_http", "duckdb_http.dialect", "DuckDBHTTPDialect")