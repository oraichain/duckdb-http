from setuptools import setup, find_packages

setup(
    name="duckdb_http",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "sqlalchemy.dialects": [
            "duckdb_http = duckdb_http:DuckDBHTTPDialect",
        ],
    },
    install_requires=["duckdb==1.3.2", "sqlalchemy==1.4.54", "requests", "sqlglot==27.6.0"],
)
