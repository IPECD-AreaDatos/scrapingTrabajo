"""
LOAD - Módulo de carga de datos DOLAR
Responsabilidad: Cargar las cotizaciones diarias a MySQL (upsert por fecha)
"""
import logging
import pymysql

logger = logging.getLogger(__name__)

TABLAS = {
    'oficial': 'dolar_oficial',
    'blue':    'dolar_blue',
    'mep':     'dolar_mep',
    'ccl':     'dolar_ccl',
}


class LoadDOLAR:
    """Carga las cotizaciones de dólar a MySQL (upsert por fecha)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database

    def load(self, datos: dict) -> bool:
        """
        Inserta o actualiza cada cotización en su tabla correspondiente.

        Returns:
            bool: True si se cargó al menos un registro
        """
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )
        cargado = False
        try:
            for tipo, df in datos.items():
                tabla = TABLAS.get(tipo)
                if tabla is None or df is None or df.empty:
                    continue
                fecha  = df['fecha'].iloc[0]
                compra = df['compra'].iloc[0]
                venta  = df['venta'].iloc[0]
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) as count FROM {tabla} WHERE fecha = %s", (fecha,))
                    if cur.fetchone()['count'] > 0:
                        cur.execute(f"DELETE FROM {tabla} WHERE fecha = %s", (fecha,))
                    cur.execute(
                        f"INSERT INTO {tabla} (fecha, compra, venta) VALUES (%s, %s, %s)",
                        (fecha, compra, venta)
                    )
                    logger.info("[LOAD] '%s' actualizado para fecha %s.", tabla, fecha)
                    cargado = True
            conn.commit()
        finally:
            conn.close()
        return cargado

    def close(self):
        pass  # No hay engine persistente en este loader
