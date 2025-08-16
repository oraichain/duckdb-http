# duckdb_http/__init__.py
import json
import requests
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy.sql.expression import text


# --- DBAPI stub ---
class DuckDBHTTPDBAPI:
    paramstyle = "pyformat"

    class Error(Exception):
        pass

    class Connection:
        def __init__(self, url, api_key=None):
            self.url = url
            self.api_key = api_key

        def cursor(self):
            return DuckDBHTTPDBAPI.Cursor(self.url, self.api_key)

        def close(self):
            pass

        def commit(self):
            pass

        def rollback(self):
            pass

    class Cursor:
        def __init__(self, url, api_key=None):
            self.url = url
            self.api_key = api_key
            self._results = []
            self._row_idx = 0
            self.description = []
            self.rowcount = 0

        def execute(self, query, parameters=None):
            if parameters:
                query = query % parameters

            headers = {"Content-Type": "text/plain"}
            if self.api_key:
                headers["X-API-Key"] = self.api_key

            resp = requests.post(self.url, data=query, headers=headers)
            resp.raise_for_status()

            raw = resp.text.strip()
            self._results = []
            self.description = []

            try:
                payload = resp.json()
            except ValueError:
                payloads = []
                for line in raw.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payloads.append(json.loads(line))
                    except ValueError:
                        continue

                if payloads and all(isinstance(p, dict) for p in payloads):
                    cols = list(payloads[0].keys())
                    self._results = [tuple(p.get(c) for c in cols) for p in payloads]
                    self.description = [(col, None, None, None, None, None, None) for col in cols]
                else:
                    self._results = payloads
                    if payloads and isinstance(payloads[0], (list, tuple)):
                        self.description = [
                            (f"col{i}", None, None, None, None, None, None)
                            for i in range(len(payloads[0]))
                        ]
                self._row_idx = 0
                self.rowcount = len(self._results)
                return self

            if isinstance(payload, dict) and not ("data" in payload or "columns" in payload):
                cols = list(payload.keys())
                row = tuple(payload.values())
                self._results = [row]
                self.description = [(col, None, None, None, None, None, None) for col in cols]
            else:
                self._results = payload.get("data", [])
                cols = payload.get("columns", [])
                types = payload.get("types", [None] * len(cols))
                if cols:
                    self.description = [
                        (col, typ, None, None, None, None, None)
                        for col, typ in zip(cols, types)
                    ]
                elif self._results:
                    num_cols = len(self._results[0])
                    self.description = [
                        (f"col{i}", None, None, None, None, None, None)
                        for i in range(num_cols)
                    ]
                else:
                    self.description = []

            self._row_idx = 0
            self.rowcount = len(self._results)
            return self

        def fetchone(self):
            if self._row_idx < len(self._results):
                row = self._results[self._row_idx]
                self._row_idx += 1
                return row
            return None

        def fetchmany(self, size=None):
            if size is None:
                size = 1
            rows = self._results[self._row_idx:self._row_idx + size]
            self._row_idx += len(rows)
            return rows

        def fetchall(self):
            if self._row_idx < len(self._results):
                rows = self._results[self._row_idx:]
                self._row_idx = len(self._results)
                return rows
            return []

        def close(self):
            self._results = []
            self.description = []
            self.rowcount = 0

    @staticmethod
    def connect(user=None, password=None, host=None, port=None, database=None, **kw):
        url = f"http://{host}:{port}/"
        api_key = password if password else None
        return DuckDBHTTPDBAPI.Connection(url, api_key)


# --- SQLAlchemy Dialect ---
class DuckDBHTTPDialect(default.DefaultDialect):
    name = "duckdb_http"
    driver = "requests"
    supports_statement_cache = True
    supports_native_boolean = True
    supports_schemas = True
    supports_native_decimal = True

    @classmethod
    def dbapi(cls):
        return DuckDBHTTPDBAPI
    
    # -----------------------------
    # Schema / Table Reflection
    # -----------------------------

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Return primary key constraint info.
        Superset expects a dict with:
        {'constrained_columns': [col1, col2], 'name': constraint_name_or_None}
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = text(f"PRAGMA table_info('{full_table}')")
        result = connection.execute(sql)
        pk_columns = [
            row[1] for row in result.fetchall() if row[5] == "true"            
        ]  # pk column is 6th field (index 5)
        return {
            "constrained_columns" : pk_columns,
            "name" : None  # DuckDB does not require a name here
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Return a list of foreign key constraints.
        DuckDB HTTP: Not supported, so return empty list.
        """
        return []
    
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Return a list of indexes for the table.
        DuckDB HTTP: indexes not supported, so return empty list.
        """
        return []
    
    def get_multi_indexes(self, connection, schema=None, filter_names=None, **kw):
        """
        Return a list of indexes for the table.
        DuckDB HTTP: indexes not supported, so return empty list.
        """
        return []


    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of view names for a schema"""
        sql = text(f"""SELECT table_name
            FROM information_schema.tables
            WHERE
                table_type='VIEW'
                AND table_schema = '{schema}'""")
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]


    def get_schema_names(self, connection, **kw):
        """Return a list of schema names"""
        sql = text("""SELECT database_name, schema_name AS nspname
              FROM duckdb_schemas() ORDER BY database_name, nspname""")
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for a schema"""
        sql = text("""
            SELECT database_name, schema_name, table_name
            FROM duckdb_tables()            
            """)
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return column info for a given table"""        
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = text(f"DESCRIBE {full_table}")
        result = connection.execute(sql)
        columns = []
        for row in result.fetchall():
            name = row[0]
            type_str = row[1].upper()
            coltype = self._map_type(type_str)
            columns.append({
                "name": name,
                "type": coltype,
                "nullable": True,
                "default": None,
                "autoincrement": False,
            })
        return columns

    # -----------------------------
    # Type mapping
    # -----------------------------
    def _map_type(self, type_str):
        if "INT" in type_str:
            return sqltypes.Integer
        elif "CHAR" in type_str or "TEXT" in type_str:
            return sqltypes.String
        elif "DOUBLE" in type_str or "FLOAT" in type_str or "DECIMAL" in type_str:
            return sqltypes.Float
        elif "BOOLEAN" in type_str:
            return sqltypes.Boolean
        elif "DATE" in type_str:
            return sqltypes.Date
        elif "TIMESTAMP" in type_str:
            return sqltypes.TIMESTAMP
        else:
            return sqltypes.String

__all__ = [
    "DuckDBHTTPDialect"
]