"""
LOAD - Módulo de carga de datos RIPTE
Responsabilidad: Insertar el nuevo valor de RIPTE si difiere del último en BD
"""
import logging
import calendar
import pymysql
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

TABLA = 'ripte'
TOLERANCIA = 100  # Diferencia mínima para considerar que hay dato nuevo


class LoadRIPTE:
    """Carga el último valor de RIPTE a MySQL si es diferente al anterior."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database

    def load(self, valor: float) -> bool:
        """
        Inserta el nuevo valor si difiere del último en BD por más de TOLERANCIA.

        Returns:
            bool: True si se insertó un nuevo registro
        """
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT fecha, valor FROM {TABLA} ORDER BY fecha DESC LIMIT 1")
                ultima_fecha, ultimo_ripte = cur.fetchone()

            logger.info("[LOAD] Último RIPTE en BD: %s = %s", ultima_fecha, ultimo_ripte)

            if abs(valor - float(ultimo_ripte)) < TOLERANCIA:
                logger.info("[LOAD] Valor RIPTE sin cambios significativos. No se insertó.")
                return False

            fecha_base = datetime.strptime(str(ultima_fecha), "%Y-%m-%d")
            nueva_fecha = fecha_base + timedelta(
                days=calendar.monthrange(fecha_base.year, fecha_base.month)[1]
            )
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {TABLA} (fecha, valor) VALUES (%s, %s)",
                            (nueva_fecha, valor))
            conn.commit()
            logger.info("[LOAD] Nuevo RIPTE insertado: %s = %s", nueva_fecha, valor)
            return True
        finally:
            conn.close()

    def close(self):
        pass
