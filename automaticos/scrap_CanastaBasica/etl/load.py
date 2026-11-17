"""
Módulo de Carga
Responsabilidad: Gestionar la tabla 'extracciones' e insertar en 'precios_productos'

La carga es solo a PostgreSQL (base `canasta_basica_super`). La base MySQL anterior
(`canasta_basica_supermercados`) fue retirada: ya no se escribe ni se lee.
"""
import os
import sys
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# Agregar directorio padre
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.utils_db import ConexionBaseDatos

logger = logging.getLogger(__name__)

class LoadCanastaBasica:
    def __init__(self):
        load_dotenv()
        # Base única: PostgreSQL
        self.db_v2 = ConexionBaseDatos(
            host=os.getenv('HOST_DBB2'),
            user=os.getenv('USER_DBB2'),
            password=os.getenv('PASSWORD_DBB2'),
            database=os.getenv('NAME_DB_CANASTA', 'canasta_basica_super'),
            port=os.getenv('PORT_DBB2', 5432)
        )

    def registrar_inicio_extraccion(self, db_instancia, nombre_log):
        """Crea la fila de 'extracciones' y devuelve su id.

        El id se calcula como MAX+1 en la misma sentencia: la secuencia de la tabla
        quedó desfasada (se insertaban ids explícitos), así que no se depende de ella.
        """
        if not db_instancia.connect_db():
            return None

        try:
            query = """
                INSERT INTO extracciones
                    (id_extraccion, fecha_inicio, estado, productos_extraidos, productos_exitosos,
                     productos_fallidos, duracion_segundos, created_at)
                VALUES
                    ((SELECT COALESCE(MAX(id_extraccion), 0) + 1 FROM extracciones),
                     NOW(), 'procesando', 0, 0, 0, 0, NOW())
                RETURNING id_extraccion
            """
            with db_instancia.connection.cursor() as cursor:
                cursor.execute(query)
                id_generado = cursor.fetchone()[0]
                db_instancia.connection.commit()
                return id_generado
        except Exception as e:
            logger.error(f"[LOAD] Error registrando extracción en {nombre_log}: {e}")
            try:
                db_instancia.connection.rollback()
            except Exception:
                pass
            return None
        finally:
            db_instancia.close_connections()

    def _ejecutar_carga_por_instancia(self, db_instancia, df, nombre_log, observaciones=None):
        """Registra la extracción, inserta los precios y deja el estado final."""
        total_productos = len(df)

        id_extraccion = self.registrar_inicio_extraccion(db_instancia, nombre_log)
        if not id_extraccion:
            logger.error(f"[LOAD] No se pudo obtener id_extraccion para {nombre_log}")
            return False, None

        df_local = df.copy()
        df_local['id_extraccion'] = id_extraccion

        if db_instancia.connect_db():
            try:
                success = db_instancia.insert_append('precios_productos', df_local)
                estado = 'completada' if success else 'error'

                query_update = """
                    UPDATE extracciones
                    SET estado = %s,
                        fecha_fin = NOW(),
                        productos_extraidos = %s,
                        productos_exitosos = %s,
                        observaciones = %s
                    WHERE id_extraccion = %s
                """
                params = (
                    estado,
                    total_productos,
                    total_productos if success else 0,
                    observaciones if observaciones else None,
                    id_extraccion,
                )
                with db_instancia.connection.cursor() as cursor:
                    cursor.execute(query_update, params)
                    db_instancia.connection.commit()

                if success:
                    logger.info(f"[OK] Carga finalizada en {nombre_log}. ID: {id_extraccion}")
                else:
                    logger.error(f"[ERROR] Inserción rechazada o incompleta en {nombre_log}. ID: {id_extraccion}")

                return success, id_extraccion
            except Exception as e:
                logger.error(f"[ERROR] Carga fallida en {nombre_log}: {e}")
                return False, id_extraccion
            finally:
                db_instancia.close_connections()
        return False, None

    def load(self, df: pd.DataFrame, observaciones: str = None) -> bool:
        if df.empty:
            logger.warning("[LOAD] DataFrame vacío, nada que cargar.")
            return False

        # --- Filtrado de precios 0 corregido ---
        total_inicial = len(df)
        if 'precio_normal' in df.columns and 'precio_descuento' in df.columns:
            df = df[~((df['precio_normal'] <= 0) & (df['precio_descuento'] <= 0))].copy()
        elif 'precio_normal' in df.columns:
            df = df[df['precio_normal'] > 0].copy()

        eliminados = total_inicial - len(df)
        if eliminados > 0:
            logger.info(f"[LOAD] Filtrado de ceros: {eliminados} productos eliminados.")

        if df.empty:
            return False

        # Desduplicación por link
        if 'id_link_producto' in df.columns:
            antes = len(df)
            df = df.drop_duplicates(subset=['id_link_producto'], keep='last').copy()
            if len(df) < antes:
                logger.warning("[LOAD] Eliminadas %d filas duplicadas (mismo id_link_producto); quedan %d.", antes - len(df), len(df))

        # Fechas
        ahora = datetime.now()
        df['fecha_extraccion'] = ahora.date()
        df['created_at'] = ahora

        logger.info("Cargando en PostgreSQL...")
        exito, id_extraccion = self._ejecutar_carga_por_instancia(
            self.db_v2, df, "PostgreSQL", observaciones=observaciones
        )
        return bool(exito and id_extraccion)
