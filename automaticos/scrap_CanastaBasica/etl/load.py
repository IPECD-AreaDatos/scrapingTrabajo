"""
Módulo de Carga
Responsabilidad: Gestionar la tabla 'extracciones' e insertar en 'precios_productos'
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
        # 1. Configuración Base Vieja (MySQL)
        self.db_v1 = ConexionBaseDatos(
            host=os.getenv('HOST_DBB1'),
            user=os.getenv('USER_DBB1'),
            password=os.getenv('PASSWORD_DBB1'),
            database=os.getenv('NAME_DB_CANASTA', 'canasta_basica_supermercados')
        )

        # 2. Configuración Base Nueva (PostgreSQL)
        self.db_v2 = ConexionBaseDatos(
            host=os.getenv('HOST_DBB2'),
            user=os.getenv('USER_DBB2'),
            password=os.getenv('PASSWORD_DBB2'),
            database=os.getenv('NAME_DB_CANASTA', 'canasta_basica_supermercados'),
            port=os.getenv('PORT_DBB2', 5432)
        )

    def registrar_inicio_extraccion(self, db_instancia, nombre_log):
        """Crea un registro en la tabla extracciones y devuelve el ID"""
        if not db_instancia.connect_db():
            logger.error(f"No se pudo conectar a {nombre_log} para registrar extracción")
            return None

        try:
            # Asumiendo tabla extracciones: id_extraccion, fecha_inicio, estado
            query = "INSERT INTO extracciones (fecha_inicio, estado) VALUES (NOW(), 'procesando')"
            
            with db_instancia.connection.cursor() as cursor:
                cursor.execute(query)
                db_instancia.connection.commit()
                
                # Intentar obtener el ID (funciona directo en MySQL)
                if hasattr(cursor, 'lastrowid'):
                    return cursor.lastrowid
                else:
                    # Alternativa para PostgreSQL si el cursor no tiene lastrowid
                    cursor.execute("SELECT LASTVAL()")
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"[LOAD] Error registrando extracción en {nombre_log}: {e}")
            return None
        finally:
            db_instancia.close_connections()

    def _ejecutar_carga_por_instancia(self, db_instancia, df, nombre_log):
        """Lógica interna de carga para evitar repetir código"""
        total_productos = len(df)
        id_extraccion = self.registrar_inicio_extraccion(db_instancia, nombre_log)
        
        if not id_extraccion:
            return False

        # Preparar DF para esta base
        df_local = df.copy()
        df_local['id_extraccion'] = id_extraccion

        if db_instancia.connect_db():
            try:
                # Inserción masiva
                success = db_instancia.insert_append('precios_productos', df_local)
                estado = 'completada' if success else 'error'
                
                # Actualizar estado
                query_update = f"""
                    UPDATE extracciones 
                    SET estado = '{estado}', 
                        fecha_fin = NOW(),
                        productos_extraidos = {total_productos}
                    WHERE id_extraccion = {id_extraccion}
                """
                with db_instancia.connection.cursor() as cursor:
                    cursor.execute(query_update)
                    db_instancia.connection.commit()
                
                logger.info(f"[OK] Carga finalizada en {nombre_log}. ID: {id_extraccion}")
                return success
            except Exception as e:
                logger.error(f"[ERROR] Carga fallida en {nombre_log}: {e}")
                return False
            finally:
                db_instancia.close_connections()
        return False

    def load(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning("[LOAD] DataFrame vacío, nada que cargar.")
            return False

        # --- Filtrado de precios 0 ---
        total_inicial = len(df)
        col_precio_1, col_precio_2 = 'precio_lista', 'precio_promo'

        if col_precio_1 in df.columns and col_precio_2 in df.columns:
            df = df[~((df[col_precio_1] == 0) & (df[col_precio_2] == 0))].copy()
        elif 'precio' in df.columns:
            df = df[df['precio'] > 0].copy()
            
        logger.info(f"[LOAD] Filtrado: {total_inicial - len(df)} productos eliminados.")

        if df.empty:
            return False

        # Agregar fecha de extracción una sola vez para ambas
        df['fecha_extraccion'] = datetime.now()

        # --- CARGA DUAL ---
        logger.info("Iniciando carga dual en Base Vieja y Base Nueva...")
        
        exito_v1 = self._ejecutar_carga_por_instancia(self.db_v1, df, "BASE VIEJA (MySQL)")
        exito_v2 = self._ejecutar_carga_por_instancia(self.db_v2, df, "BASE NUEVA (Postgres)")

        return exito_v1 and exito_v2