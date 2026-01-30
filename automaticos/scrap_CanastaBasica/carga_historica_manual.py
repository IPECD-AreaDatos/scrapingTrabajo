import os
import sys
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# 1. Configuración de Rutas
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from utils.utils_db import ConexionBaseDatos

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE ARCHIVOS ---
ARCHIVOS_CSV = [
    'BACKUP_RAW_20260128_2316.csv',
    'BACKUP_RAW_20260130_0058.csv'
]
CARPETA_CSV = ''  

class CargaHistorica:
    def __init__(self):
        load_dotenv()
        self.db = ConexionBaseDatos(
            host=os.getenv('HOST_DBB'),
            user=os.getenv('USER_DBB'),
            password=os.getenv('PASSWORD_DBB'),
            database='canasta_basica_supermercados'
        )

    def obtener_links_validos(self):
        # Solo traemos el link para validar existencia
        sql = "SELECT link FROM link_productos"
        return pd.read_sql(sql, self.db.engine)

    def crear_extraccion_manual(self, nombre_archivo, fecha_archivo):
        observacion = f"Carga Manual Historica: {nombre_archivo}"
        sql = f"""
            INSERT INTO extracciones (fecha_inicio, fecha_fin, estado, observaciones, created_at)
            VALUES ('{fecha_archivo} 12:00:00', '{fecha_archivo} 12:00:00', 'procesando', '{observacion}', NOW())
        """
        with self.db.connection.cursor() as cursor:
            cursor.execute(sql)
            self.db.connection.commit()
            return cursor.lastrowid

    def cerrar_extraccion(self, id_extraccion, cantidad, estado='completada'):
        sql = f"""
            UPDATE extracciones 
            SET estado = '{estado}', productos_extraidos = {cantidad}
            WHERE id_extraccion = {id_extraccion}
        """
        with self.db.connection.cursor() as cursor:
            cursor.execute(sql)
            self.db.connection.commit()

    def procesar_y_cargar(self):
        if not self.db.connect_db():
            logger.error("No se pudo conectar a la BD")
            return

        logger.info("Obteniendo mapa de links existentes para validación...")
        df_links_db = self.obtener_links_validos()
        
        for archivo in ARCHIVOS_CSV:
            ruta_completa = os.path.join(current_dir, CARPETA_CSV, archivo)
            if not os.path.exists(ruta_completa):
                logger.error(f"No encuentro el archivo: {ruta_completa}")
                continue

            logger.info(f"--- Procesando {archivo} ---")
            
            try:
                # --- NUEVA LÓGICA DE FECHAS DESDE EL NOMBRE DEL ARCHIVO ---
                # Formato esperado: BACKUP_RAW_YYYYMMDD_HHMM.csv
                # Ejemplo: BACKUP_RAW_20260129_2001.csv
                partes = archivo.split('_') 
                fecha_str_archivo = partes[2]  # 20260129
                hora_str_archivo = partes[3].replace('.csv', '') # 2001
                
                # Creamos el objeto datetime real del archivo
                fecha_real_dt = datetime.strptime(f"{fecha_str_archivo} {hora_str_archivo}", "%Y%m%d %H%M")
                
                # Formateamos para SQL (String)
                fecha_sql_completa = fecha_real_dt.strftime('%Y-%m-%d %H:%M:%S') # Para created_at
                fecha_sql_dia = fecha_real_dt.strftime('%Y-%m-%d')               # Para fecha_extraccion y crear extracción

                logger.info(f"Fecha detectada del archivo: {fecha_sql_completa}")

                # 1. Leer CSV
                df_csv = pd.read_csv(ruta_completa)
                
                # Limpieza de columnas basura si existen
                if 'unidad_medida' in df_csv.columns:
                    df_csv.drop(columns=['unidad_medida'], inplace=True)

                # 2. Merge (Validación)
                df_merged = pd.merge(
                    df_csv, 
                    df_links_db, 
                    left_on='url', 
                    right_on='link', 
                    how='inner'
                )

                if df_merged.empty:
                    logger.warning(f"El archivo {archivo} no cruzó con ningún link conocido.")
                    continue

                # Eliminar duplicados
                df_merged.drop_duplicates(subset=['id_link_producto'], keep='first', inplace=True)

                # 4. Crear Extracción (Usando la fecha del archivo)
                id_extraccion = self.crear_extraccion_manual(archivo, fecha_sql_dia)
                df_merged['id_extraccion'] = id_extraccion

                # 5. Mapeo y Limpieza
                rename_map = {
                    'nombre': 'nombre_producto',
                    'unidad': 'unidad_medida'
                }
                df_merged.rename(columns=rename_map, inplace=True)

                # Correcciones ortográficas
                correcciones = {
                    "Carnae picada": "Carne picada",
                    "Carnaza comun": "Carnaza común",
                    "Harina de maiz": "Harina de maíz",
                    "Higado": "Hígado",
                    "Leche liquida": "Leche líquida",
                    "Pan Frances": "Pan Francés"
                }
                for mal, bien in correcciones.items():
                    df_merged['nombre_producto'] = df_merged['nombre_producto'].str.replace(mal, bien, regex=False, case=False)

                # --- CORRECCIÓN DE FECHAS EN EL DATAFRAME ---
                # En lugar de usar datetime.now(), asignamos la fecha del archivo a TODA la columna.
                # Esto también soluciona los [NULL] que veías, porque forzamos el valor.
                df_merged['fecha_extraccion'] = fecha_sql_dia
                df_merged['created_at'] = fecha_sql_completa

                # 6. Selección final de columnas
                cols_destino = [
                    'id_link_producto', 'id_extraccion', 'nombre_producto',
                    'precio_normal', 'precio_descuento', 'precio_por_unidad',
                    'unidad_medida', 'peso', 'created_at', 'fecha_extraccion'
                ]
                
                for col in cols_destino:
                    if col not in df_merged.columns:
                        df_merged[col] = None

                df_final = df_merged[cols_destino].copy()
                df_final['precio_descuento'] = df_final['precio_descuento'].fillna(0)

                # 7. Insertar
                logger.info(f"Insertando {len(df_final)} registros con fecha {fecha_sql_completa}...")
                df_final.to_sql(
                    'precios_productos', 
                    con=self.db.engine, 
                    if_exists='append', 
                    index=False
                )
                
                self.cerrar_extraccion(id_extraccion, len(df_final), 'completada')
                logger.info("¡Éxito! Carga completada.")

            except Exception as e:
                logger.error(f"Error procesando {archivo}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                if 'id_extraccion' in locals():
                      self.cerrar_extraccion(id_extraccion, 0, 'error')

        self.db.close_connections()

if __name__ == "__main__":
    cargador = CargaHistorica()
    cargador.procesar_y_cargar()