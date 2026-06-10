"""Loader para extraer datos desde SQL Server (vista V_EmbarazosDW)."""

import logging
from contextlib import contextmanager
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class ExternalDBLoader:
    """Conecta a SQL Server y extrae datos de una vista."""

    def __init__(
        self,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        port: int = 1433,
        driver: str = "ODBC Driver 17 for SQL Server",
        encrypt: str = "no",
        connection_string: str = None,
    ):
        if connection_string:
            self.connection_string = connection_string
        else:
            if not all([host, user, password]):
                raise ValueError(
                    "Debe proporcionar connection_string o los parámetros "
                    "host, user, password"
                )
            db_part = f"DATABASE={database};" if database else ""
            odbc = "".join([
                f"DRIVER={{{driver}}};",
                f"SERVER={host},{port};",
                db_part,
                f"UID={user};",
                f"PWD={password};",
                f"Encrypt={encrypt};",
                "TrustServerCertificate=yes;",
            ])
            self.connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"

        self._engine = None

    def _get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
            )
            logger.info("Engine SQL Server externo creado")
        return self._engine

    @contextmanager
    def get_connection(self):
        engine = self._get_engine()
        conn = engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def load_view(self, view_name: str, where_clause: str = None) -> pd.DataFrame:
        query = f"SELECT * FROM {view_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        logger.info(f"Consultando vista externa: {view_name}")
        df = pd.read_sql_query(text(query), self._get_engine())
        logger.info(f"  -> {len(df)} filas cargadas")
        return df

    def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1")).fetchone()
            return True
        except Exception as exc:
            logger.error(f"Error de conexión SQL Server: {exc}")
            return False


class MockExternalDBLoader:
    """Mock para testing local sin conexión real."""

    def load_view(self, view_name: str) -> pd.DataFrame:
        logger.warning(f"MOCK: devolviendo dataset vacío para {view_name}")
        return pd.DataFrame(
            columns=[
                "DNI",
                "FechaProbableParto",
                "Apellido",
                "Nombre",
                "Controles",
                "Reg F.H",
            ]
        )

    def test_connection(self) -> bool:
        return True
