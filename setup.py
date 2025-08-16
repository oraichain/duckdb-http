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
    install_requires=["sqlalchemy==1.4.54", "requests"],
)
