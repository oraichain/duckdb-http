import json
import requests
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy.sql.expression import text
from sqlalchemy.engine.reflection import cache
from sqlglot import parse_one, exp

def is_read_only(sql: str) -> bool:
    tree = parse_one(sql, error_level="ignore")
    if tree is None:
        return False

    # Regular SELECT / UNION queries
    if isinstance(tree, (exp.Select, exp.Union)):
        return True

    # SHOW / PRAGMA / EXPLAIN and similar commands
    if isinstance(tree, exp.Command):
        cmd = (tree.name or "").upper()
        if cmd in {"SHOW", "PRAGMA", "EXPLAIN"}:
            return True

    return False

# --- DBAPI stub ---
class DuckDBHTTPDBAPI:
    paramstyle = "pyformat"

    class Error(Exception):
        pass

    class Connection:
        def __init__(self, url, api_key=None, read_only=False):
            self.url = url
            self.api_key = api_key
            self.read_only = read_only

        def cursor(self):
            return DuckDBHTTPDBAPI.Cursor(self.url, self.api_key, self.read_only)

        def close(self):
            pass

        def commit(self):
            pass

        def rollback(self):
            pass

    class Cursor:
        def __init__(self, url, api_key=None, read_only=False):
            self.url = url
            self.api_key = api_key
            self.read_only = read_only
            self._results = []
            self._row_idx = 0
            self.description = []
            self.rowcount = 0

        def execute(self, query, parameters=None):
            if parameters:
                query = query % parameters

            # support read-only
            if self.read_only and not is_read_only(query):
                raise PermissionError(f"Blocked non-read query: {query}")

            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key            

            resp = requests.post(self.url, data=query, headers=headers)
            resp.raise_for_status()

            try:
                payloads = resp.json()  # JSON array
            except ValueError:
                # line-delimited JSON
                lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
                payloads = [json.loads(line) for line in lines if line]

            self._process_payloads(payloads)
            return self

        def _process_payloads(self, payloads):
            self._results, self.description = [], []

            if not payloads:
                self.rowcount = 0
                self._row_idx = 0
                return

            if isinstance(payloads, dict):
                payloads = [payloads]

            if all(isinstance(p, dict) for p in payloads):
                cols = list(payloads[0].keys())
                self._results = [tuple(p.get(c) for c in cols) for p in payloads]
                self.description = [(col, None, None, None, None, None, None) for col in cols]
            elif all(isinstance(p, (list, tuple)) for p in payloads):
                self._results = [tuple(p) for p in payloads]
                self.description = [(f"col{i}", None, None, None, None, None, None)
                                    for i in range(len(self._results[0]))]
            else:
                self._results = [(str(p),) for p in payloads]
                self.description = [("col0", None, None, None, None, None, None)]

            self.rowcount = len(self._results)
            self._row_idx = 0

        def fetchone(self):
            if self._row_idx < self.rowcount:
                row = self._results[self._row_idx]
                self._row_idx += 1
                return row
            return None

        def fetchmany(self, size=1):
            rows = self._results[self._row_idx:self._row_idx + size]
            self._row_idx += len(rows)
            return rows

        def fetchall(self):
            rows = self._results[self._row_idx:]
            self._row_idx = self.rowcount
            return rows

        def close(self):
            self._results = []
            self.description = []
            self.rowcount = 0
            self._row_idx = 0

    @staticmethod
    def connect(username=None, password=None, host=None, port=None, **kw):                
        full_host = f"{username}:{password}@{host}" if username and password else host
        url = f"http://{full_host}:{port}/"        
        return DuckDBHTTPDBAPI.Connection(url, kw.get("api_key"), (kw.get("read_only") or "").lower() == "true")


# --- SQLAlchemy Dialect ---
class DuckDBHTTPDialect(default.DefaultDialect):
    name = "duckdb_http"
    driver = "requests"
    supports_statement_cache = True
    supports_native_boolean = True
    supports_schemas = True
    supports_native_decimal = True

    # Expand type mapping so your schema works well
    _type_map = {
        # Numeric
        "TINYINT": sqltypes.SmallInteger,     # 1-byte signed int
        "SMALLINT": sqltypes.SmallInteger,    # 2-byte signed int
        "INT2": sqltypes.SmallInteger,        # alias
        "INTEGER": sqltypes.Integer,          # 4-byte signed int
        "INT4": sqltypes.Integer,             # alias
        "BIGINT": sqltypes.BigInteger,        # 8-byte signed int
        "INT8": sqltypes.BigInteger,          # alias
        "UBIGINT": sqltypes.BigInteger,       # unsigned bigint
        "UTINYINT": sqltypes.Integer,         # unsigned tinyint
        "USMALLINT": sqltypes.Integer,        # unsigned smallint
        "UINTEGER": sqltypes.Integer,         # unsigned integer
        "HUGEINT": sqltypes.Numeric,          # 128-bit signed
        "UHUGEINT": sqltypes.Numeric,         # 128-bit unsigned
        "DECIMAL": sqltypes.Numeric,          # alias NUMERIC
        "NUMERIC": sqltypes.Numeric,
        "REAL": sqltypes.Float,               # 4-byte float
        "FLOAT4": sqltypes.Float,             # alias
        "DOUBLE": sqltypes.Float,             # 8-byte float
        "FLOAT8": sqltypes.Float,             # alias
        "FLOAT": sqltypes.Float,              # alias DOUBLE

        # Boolean
        "BOOLEAN": sqltypes.Boolean,

        # Character / String
        "CHAR": sqltypes.CHAR,
        "VARCHAR": sqltypes.String,
        "STRING": sqltypes.String,
        "TEXT": sqltypes.Text,

        # Date & Time
        "DATE": sqltypes.Date,
        "TIME": sqltypes.Time,
        "TIMESTAMP": sqltypes.TIMESTAMP,
        "DATETIME": sqltypes.DateTime,
        "TIMESTAMP WITH TIME ZONE": sqltypes.TIMESTAMP(timezone=True),
        "TIMESTAMPTZ": sqltypes.TIMESTAMP(timezone=True),
        "INTERVAL": sqltypes.Interval,

        # Binary
        "BLOB": sqltypes.LargeBinary,
        "BYTEA": sqltypes.LargeBinary,

        # JSON
        "JSON": sqltypes.JSON,

        # Spatial (DuckDB has PostGIS-style)
        "GEOMETRY": sqltypes.String,  # could be custom type if needed
        "GEOGRAPHY": sqltypes.String,

        # Special / Complex
        "UUID": sqltypes.String(36), 
        "MAP": sqltypes.JSON,         # map<k,v>
        "ARRAY": sqltypes.ARRAY(sqltypes.String),  # fallback, refine later
        "STRUCT": sqltypes.JSON,      # nested struct -> JSON
        "UNION": sqltypes.JSON,       # variant type

        # Aliases
        "INT": sqltypes.Integer,
    }

    @classmethod
    def dbapi(cls):
        return DuckDBHTTPDBAPI

    @staticmethod
    def _map_type(type_str):
        type_str = type_str.upper()
        for key, typ in DuckDBHTTPDialect._type_map.items():
            if key in type_str:
                return typ
        return sqltypes.String

    # -----------------------------
    # Schema / Table Reflection
    # -----------------------------
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = text(f"PRAGMA table_info('{full_table}')")
        result = connection.execute(sql)
        pk_columns = [row[1] for row in result.fetchall() if row[5] == "true"]
        return {"constrained_columns": pk_columns, "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_multi_indexes(self, connection, schema=None, filter_names=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        sql = text(f"SELECT table_name FROM information_schema.tables WHERE table_type='VIEW' AND table_schema = '{schema}'")
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]

    @cache # type: ignore[call-arg]
    def get_schema_names(self, connection, **kw):
        sql = text("SELECT database_name, schema_name AS nspname FROM duckdb_schemas() ORDER BY database_name, nspname")
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]

    @cache # type: ignore[call-arg]
    def get_table_names(self, connection, schema=None, **kw):
        sql = text("SELECT database_name, schema_name, table_name FROM duckdb_tables()")
        result = connection.execute(sql)
        return [row[0] for row in result.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kw):        
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = text(f"DESCRIBE {full_table}")
        result = connection.execute(sql)
        columns = []
        for row in result.fetchall():            
            coltype = self._map_type(row[1])
            columns.append({
                "name": row[0],
                "type": coltype,
                "nullable": True,
                "default": None,
                "autoincrement": False,
            })
        return columns


__all__ = ["DuckDBHTTPDialect"]
