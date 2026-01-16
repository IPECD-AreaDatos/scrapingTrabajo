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
        self.db = ConexionBaseDatos(
            host=os.getenv('HOST_DBB'),
            user=os.getenv('USER_DBB'),
            password=os.getenv('PASSWORD_DBB'),
            database='canasta_basica_supermercados'
        )

    def registrar_inicio_extraccion(self):
        """Crea un registro en la tabla extracciones y devuelve el ID"""
        if not self.db.connect_db():
            logger.error("No se pudo conectar para registrar extracción")
            return None

        try:
            # Asumiendo tabla extracciones: id_extraccion, fecha_inicio, estado
            query = "INSERT INTO extracciones (fecha_inicio, estado) VALUES (NOW(), 'procesando')"
            
            with self.db.connection.cursor() as cursor:
                cursor.execute(query)
                self.db.connection.commit()
                extraccion_id = cursor.lastrowid
                logger.info(f"[LOAD] Nueva extracción registrada. ID: {extraccion_id}")
                return extraccion_id
        except Exception as e:
            logger.error(f"[LOAD] Error registrando extracción: {e}")
            return None
        finally:
            self.db.close_connections()

    def load(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning("[LOAD] DataFrame vacío, nada que cargar.")
            return False

        # --- CORRECCIÓN 1: Filtrar productos con precio 0 ---
        # Guardamos la cantidad inicial para el log (opcional)
        total_inicial = len(df)
        
        # Filtramos: Nos quedamos solo con los que tengan AL MENOS un precio mayor a 0.
        # NOTA: Ajusta 'precio_regular' y 'precio_promo' a los nombres reales de tus columnas.
        # Si tu lógica es "ambos deben ser 0 para eliminar", usamos esta condición:
        # "Eliminar si precio_regular es 0 Y precio_promo es 0"
        
        # Ejemplo asumiendo que tus columnas se llaman 'precio_lista' y 'precio_promo'
        # (Ajusta estos nombres según tu DataFrame real)
        col_precio_1 = 'precio_lista' 
        col_precio_2 = 'precio_promo'

        # Verifica si las columnas existen para evitar errores
        if col_precio_1 in df.columns and col_precio_2 in df.columns:
            df = df[~((df[col_precio_1] == 0) & (df[col_precio_2] == 0))].copy()
        elif 'precio' in df.columns: # Caso de respaldo si solo hay una columna
            df = df[df['precio'] > 0].copy()
            
        productos_validos = len(df)
        logger.info(f"[LOAD] Filtrado: {total_inicial - productos_validos} productos con precio 0 eliminados.")

        if df.empty:
            logger.warning("[LOAD] Todos los productos tenían precio 0. Nada que cargar.")
            return False

        # --- CORRECCIÓN 2: Agregar fecha de extracción ---
        # Asignamos la fecha y hora actual a la columna 'fecha_extraccion'
        df['fecha_extraccion'] = datetime.now()

        # 1. Registrar la extracción (Batch ID)
        id_extraccion = self.registrar_inicio_extraccion()
        if not id_extraccion:
            return False

        # 2. Agregar el ID al DataFrame
        df['id_extraccion'] = id_extraccion

        # 3. Insertar en precios_productos
        logger.info(f"[LOAD] Insertando {productos_validos} precios válidos en BD...")
        
        if self.db.connect_db():
            try:
                # Usar to_sql via sqlalchemy engine del utils
                success = self.db.insert_append('precios_productos', df)
                
                # Actualizar estado de la extracción
                estado = 'completada' if success else 'error'
                
                # Actualizar contadores en la tabla extracciones
                # AHORA usamos 'productos_validos' que ya tiene descontados los de precio 0
                
                query_update = f"""
                    UPDATE extracciones 
                    SET estado = '{estado}', 
                        fecha_fin = NOW(),
                        productos_extraidos = {productos_validos}
                    WHERE id_extraccion = {id_extraccion}
                """
                
                with self.db.connection.cursor() as cursor:
                    cursor.execute(query_update)
                    self.db.connection.commit()
                
                return success
            except Exception as e:
                logger.error(f"[LOAD] Error fatal en carga masiva: {e}")
                return False
            finally:
                self.db.close_connections()
        return False