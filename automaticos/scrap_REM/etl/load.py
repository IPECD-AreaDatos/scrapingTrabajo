"""
LOAD - Módulo de carga de datos REM
Responsabilidad: Cargar precios minoristas y cambio nominal a MySQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadREM:
    """Carga los datos del REM a MySQL/PostgreSQL de forma híbrida."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None
        self.tabla_cambio = "rem_precios_minoristas"

    def _conectar(self):
        """Crea el motor de conexión dinámico."""
        if self.engine is None:
            if self.version == "1":
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a '{self.database}' (v{self.version})")

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def load(self, df_new: pd.DataFrame):
        """Carga la tabla de cambio nominal con Truncate + Replace."""
        self._conectar()
        schema = self._get_schema()

        try:
            # 1. Intentamos leer la última fecha de consulta cargada
            query = f"SELECT MAX(fecha_consulta) FROM {self.tabla_cambio}"
            with self.engine.connect() as conn:
                last_date = conn.execute(text(query)).scalar()

            if last_date is not None:
                # 2. Traemos la última carga para comparar
                query_last_data = f"SELECT fecha, mediana FROM {self.tabla_cambio} WHERE fecha_consulta = :last_date"
                df_old = pd.read_sql(text(query_last_data), self.engine, params={"last_date": last_date})
                
                # Convertimos fechas a datetime para comparar bien
                df_old['fecha'] = pd.to_datetime(df_old['fecha'])
                
                # 3. Comparación: ¿Son iguales la fecha y la mediana?
                # Ordenamos ambos para que la comparación sea justa
                check_columns = ['fecha', 'mediana']
                df_new_sorted = df_new[check_columns].sort_values('fecha').reset_index(drop=True)
                df_old_sorted = df_old[check_columns].sort_values('fecha').reset_index(drop=True)

                if df_new_sorted.equals(df_old_sorted):
                    logger.info("[LOAD] Los datos son idénticos a la última carga del %s. No se realiza el insert.", last_date)
                    return # Cortamos la ejecución aquí

            # 4. Si no son iguales o la tabla está vacía, cargamos
            with self.engine.begin() as conn:
                df_new.to_sql(
                    name=self.tabla_cambio, 
                    con=conn, 
                    schema=schema, 
                    if_exists='append', # IMPORTANTE: ahora es append
                    index=False, 
                    method='multi'
                )
            logger.info("[LOAD] Se detectaron cambios. '%s' actualizada con %d filas.", self.tabla_cambio, len(df_new))

        except Exception as e:
            # Si la tabla no existe (primera vez), la creamos directamente
            logger.warning("[LOAD] Error al comparar (posible tabla nueva): %s", e)
            df_new.to_sql(name=self.tabla_cambio, con=self.engine, schema=schema, if_exists='append', index=False)

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None