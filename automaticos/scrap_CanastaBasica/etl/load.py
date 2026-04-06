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
            database=os.getenv('NAME_DB_CANASTA_V1', 'canasta_basica_supermercados')
        )

        # 2. Configuración Base Nueva (PostgreSQL)
        self.db_v2 = ConexionBaseDatos(
            host=os.getenv('HOST_DBB2'),
            user=os.getenv('USER_DBB2'),
            password=os.getenv('PASSWORD_DBB2'),
            database=os.getenv('NAME_DB_CANASTA', 'canasta_basica_super'),
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
                if hasattr(cursor, 'lastrowid') and cursor.lastrowid:
                    return cursor.lastrowid
                else:
                    # Alternativa para PostgreSQL si el cursor no tiene lastrowid
                    cursor.execute("SELECT LASTVAL()")
                    result = cursor.fetchone()
                    return result[0] if result else None
        except Exception as e:
            logger.error(f"[LOAD] Error registrando extracción en {nombre_log}: {e}")
            return None
        finally:
            db_instancia.close_connections()

    def _ejecutar_carga_por_instancia(self, db_instancia, df, nombre_log, id_extraccion_propuesto=None):
        """Lógica interna de carga para evitar repetir código"""
        total_productos = len(df)
        
        # Si se propone un ID (ej: capturado de MySQL), intentamos usarlo en Postgres
        if id_extraccion_propuesto:
            id_extraccion = id_extraccion_propuesto
            # Se debe asegurar que este ID exista en la tabla extracciones de la nueva base
            if not self._registrar_id_especifico_extraccion(db_instancia, id_extraccion, nombre_log):
                return False, None
        else:
            id_extraccion = self.registrar_inicio_extraccion(db_instancia, nombre_log)
        
        if not id_extraccion:
            logger.error(f"[LOAD] No se pudo obtener id_extraccion para {nombre_log}")
            return False, None

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
                return success, id_extraccion
            except Exception as e:
                logger.error(f"[ERROR] Carga fallida en {nombre_log}: {e}")
                return False, id_extraccion
            finally:
                db_instancia.close_connections()
        return False, None

    def _registrar_id_especifico_extraccion(self, db_instancia, id_extraccion, nombre_log):
        """Intenta registrar un ID específico en la tabla extracciones (para mantener simetría)"""
        if not db_instancia.connect_db():
            return False
        try:
            # Primero verificamos si ya existe (para evitar errores si ya se registró)
            query_check = f"SELECT id_extraccion FROM extracciones WHERE id_extraccion = {id_extraccion}"
            with db_instancia.connection.cursor() as cursor:
                cursor.execute(query_check)
                if cursor.fetchone():
                    return True
                
                # Si no existe, lo insertamos
                # Si es SERIAL en Postgres, esto puede requerir OVERRIDING SYSTEM VALUE pero usualmente INSERT directo funciona si no choca
                query_insert = f"INSERT INTO extracciones (id_extraccion, fecha_inicio, estado) VALUES ({id_extraccion}, NOW(), 'procesando')"
                cursor.execute(query_insert)
                db_instancia.connection.commit()
                return True
        except Exception as e:
            logger.error(f"[LOAD] Error registrando ID específico en {nombre_log}: {e}")
            return False
        finally:
            db_instancia.close_connections()

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
        
        # 1. Carga en Base Vieja (MySQL) - OBLIGATORIA
        exito_v1, id_extraccion = self._ejecutar_carga_por_instancia(self.db_v1, df, "BASE VIEJA (MySQL)")
        
        if not exito_v1 or not id_extraccion:
            logger.error("[GUARD] Carga en Base Vieja falló o no generó ID. ABORTANDO Postgres.")
            return False

        # 2. Carga en Base Nueva (Postgres) - Solo si la primera fue exitosa
        # Pasamos el mismo id_extraccion capturado de MySQL
        exito_v2, _ = self._ejecutar_carga_por_instancia(self.db_v2, df, "BASE NUEVA (Postgres)", id_extraccion_propuesto=id_extraccion)

        return exito_v1 and exito_v2