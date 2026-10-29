"""
Módulo de Carga
Responsabilidad: Gestionar la tabla 'extracciones' e insertar en 'precios_productos'
"""
import os
import sys
import pandas as pd
import logging
from sqlalchemy import text
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
        if not db_instancia.connect_db():
            return None

        try:
            # Definimos las columnas que Postgres exige que no sean NULL
            columnas = "(fecha_inicio, estado, productos_extraidos, productos_exitosos, productos_fallidos, duracion_segundos, created_at)"
            valores = "(NOW(), 'procesando', 0, 0, 0, 0, NOW())"

            if db_instancia.engine.name == 'postgresql':
                # Query para Postgres con RETURNING
                query = f"INSERT INTO extracciones {columnas} VALUES {valores} RETURNING id_extraccion"
            else:
                # Query para MySQL (normal)
                query = f"INSERT INTO extracciones {columnas} VALUES {valores}"
            
            with db_instancia.connection.cursor() as cursor:
                cursor.execute(query)
                
                if db_instancia.engine.name == 'postgresql':
                    id_generado = cursor.fetchone()[0] # El ID viene del RETURNING
                else:
                    id_generado = cursor.lastrowid # Clásico de MySQL
                
                db_instancia.connection.commit()
                try:
                    is_pg = (db_instancia.engine.name == 'postgresql')
                    alter_query = "ALTER TABLE extracciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;" if is_pg else "ALTER TABLE extracciones ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                    with db_instancia.connection.cursor() as cursor_alter:
                        cursor_alter.execute(alter_query)
                    db_instancia.connection.commit()
                except Exception as e:
                    logger.warning(f"[LOAD] No se pudo agregar la columna updated_at a extracciones: {e}")
                return id_generado
        except Exception as e:
            logger.error(f"[LOAD] Error registrando extracción en {nombre_log}: {e}")
            return None
        finally:
            db_instancia.close_connections()

    def _ejecutar_carga_por_instancia(self, db_instancia, df, nombre_log, id_extraccion_propuesto=None, observaciones=None):
        """Lógica interna de carga para evitar repetir código"""
        total_productos = len(df)
        
        if id_extraccion_propuesto:
            id_extraccion = id_extraccion_propuesto
            if not self._registrar_id_especifico_extraccion(db_instancia, id_extraccion, nombre_log):
                return False, None
        else:
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
                
                # Manejo seguro del texto de observaciones para SQL
                obs_sql = f"'{observaciones.replace("'", "''")}'" if observaciones else "NULL"
                
                # Actualizar estado y observaciones
                query_update = f"""
                    UPDATE extracciones 
                    SET estado = '{estado}', 
                        fecha_fin = NOW(),
                        productos_extraidos = {total_productos},
                        productos_exitosos = {total_productos if success else 0},
                        observaciones = {obs_sql}
                    WHERE id_extraccion = {id_extraccion}
                """
                with db_instancia.connection.cursor() as cursor:
                    cursor.execute(query_update)
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

    def _registrar_id_especifico_extraccion(self, db_instancia, id_extraccion, nombre_log):
        """Intenta registrar un ID específico en la tabla extracciones para mantener simetría"""
        if not db_instancia.connect_db():
            return False
        try:
            # 1. Verificamos si ya existe
            query_check = f"SELECT id_extraccion FROM extracciones WHERE id_extraccion = {id_extraccion}"
            with db_instancia.connection.cursor() as cursor:
                cursor.execute(query_check)
                if cursor.fetchone():
                    return True
                
                # 2. Si no existe, lo insertamos con todos los campos obligatorios
                # Agregamos los ceros y NOW() para que Postgres no rebote
                columnas = "(id_extraccion, fecha_inicio, estado, productos_extraidos, productos_exitosos, productos_fallidos, duracion_segundos, created_at)"
                valores = f"({id_extraccion}, NOW(), 'procesando', 0, 0, 0, 0, NOW())"
                
                query_insert = f"INSERT INTO extracciones {columnas} VALUES {valores}"
                
                cursor.execute(query_insert)
                db_instancia.connection.commit()
                try:
                    is_pg = (db_instancia.engine.name == 'postgresql')
                    alter_query = "ALTER TABLE extracciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;" if is_pg else "ALTER TABLE extracciones ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                    with db_instancia.connection.cursor() as cursor_alter:
                        cursor_alter.execute(alter_query)
                    db_instancia.connection.commit()
                except Exception as e:
                    logger.warning(f"[LOAD] No se pudo agregar la columna updated_at a extracciones: {e}")
                return True
        except Exception as e:
            logger.error(f"[LOAD] Error registrando ID específico en {nombre_log}: {e}")
            return False
        finally:
            db_instancia.close_connections()

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

        # --- CARGA DUAL ---
        logger.info("Iniciando carga dual en Base Vieja y Base Nueva...")
        
        # 1. Base Vieja (MySQL)
        exito_v1, id_extraccion = self._ejecutar_carga_por_instancia(
            self.db_v1, df, "BASE VIEJA (MySQL)", observaciones=observaciones
        )
        if not exito_v1 or not id_extraccion:
            logger.error("[GUARD] Carga en Base Vieja falló. ABORTANDO Postgres.")
            return False

        # 2. Base Nueva (Postgres)
        exito_v2, _ = self._ejecutar_carga_por_instancia(
            self.db_v2, df, "BASE NUEVA (Postgres)", 
            id_extraccion_propuesto=id_extraccion, 
            observaciones=observaciones
        )

        return exito_v1 and exito_v2
    
    def sync_mysql_to_postgres(self):
        """
        Sincroniza registros de precios_productos desde MySQL a Postgres
        basado en el ID máximo (Estrategia 1).
        """
        # --- AGREGÁ ESTO PARA INICIALIZAR LOS ENGINES ---
        self.db_v1.connect_db() # Inicializa MySQL
        self.db_v2.connect_db() # Inicializa Postgres
        # ------------------------------------------------
        logger.info("Iniciando sincronización incremental MySQL -> Postgres...")
        
        try:
            # --- 1. SINCRONIZAR TABLA 'extracciones' PRIMERO ---
            with self.db_v2.engine.connect() as conn:
                last_ext_id = conn.execute(text("SELECT MAX(id_extraccion) FROM extracciones")).scalar() or 0
            
            df_ext_nuevas = pd.read_sql(f"SELECT * FROM extracciones WHERE id_extraccion > {last_ext_id}", self.db_v1.engine)
            
            if not df_ext_nuevas.empty:
                logger.info(f"Sincronizando {len(df_ext_nuevas)} registros en tabla 'extracciones'...")
                df_ext_nuevas.to_sql('extracciones', self.db_v2.engine, if_exists='append', index=False)
                try:
                    with self.db_v2.engine.begin() as conn:
                        is_pg = (self.db_v2.engine.name == 'postgresql')
                        alter_query = "ALTER TABLE extracciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;" if is_pg else "ALTER TABLE extracciones ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                        conn.execute(text(alter_query))
                except Exception as e:
                    logger.warning(f"[LOAD] No se pudo agregar la columna updated_at a extracciones: {e}")

            # --- 2. SINCRONIZAR TABLA 'precios_productos' ---
            with self.db_v2.engine.connect() as conn:
                last_precio_id = conn.execute(text("SELECT MAX(id_precio_producto) FROM precios_productos")).scalar() or 0
            
            df_precios_nuevos = pd.read_sql(f"SELECT * FROM precios_productos WHERE id_precio_producto > {last_precio_id}", self.db_v1.engine)
            
            if df_precios_nuevos.empty:
                logger.info("No hay precios nuevos para sincronizar.")
                return 0

            logger.info(f"Sincronizando {len(df_precios_nuevos)} registros en tabla 'precios_productos'...")
            df_precios_nuevos.to_sql('precios_productos', self.db_v2.engine, if_exists='append', index=False)
            try:
                with self.db_v2.engine.begin() as conn:
                    is_pg = (self.db_v2.engine.name == 'postgresql')
                    alter_query = "ALTER TABLE precios_productos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;" if is_pg else "ALTER TABLE precios_productos ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                    conn.execute(text(alter_query))
            except Exception as e:
                logger.warning(f"[LOAD] No se pudo agregar la columna updated_at a precios_productos: {e}")
            
            logger.info("Sincronización completada exitosamente.")
            return len(df_precios_nuevos)

        except Exception as e:
            logger.error(f"Error en la sincronización incremental: {e}")
            return 0
        finally:
            self.db_v1.close_connections()
            self.db_v2.close_connections()