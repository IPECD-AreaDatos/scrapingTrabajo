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
    'BACKUP_RAW_20260123_0040.csv'
]
# Carpeta donde están los archivos CSV
CARPETA_CSV = 'files'  

class CargaHistorica:
    def __init__(self):
        load_dotenv()
        self.db = ConexionBaseDatos(
            host=os.getenv('HOST_DBB'),
            user=os.getenv('USER_DBB'),
            password=os.getenv('PASSWORD_DBB'),
            database='canasta_basica_supermercados'
        )

    def obtener_mapa_links(self):
        sql = "SELECT id_link_producto, link FROM link_productos"
        return pd.read_sql(sql, self.db.engine)

    def crear_extraccion_manual(self, nombre_archivo, fecha_archivo):
        """
        Crea la extracción usando la FECHA REAL del archivo para el registro,
        aunque el sistema ponga NOW() en created_at.
        """
        observacion = f"Carga Manual Historica: {nombre_archivo}"
        # Insertamos con la fecha del archivo como fecha_inicio para mantener coherencia cronológica visual
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

        logger.info("Obteniendo mapa de links existentes...")
        df_links_db = self.obtener_mapa_links()
        
        for archivo in ARCHIVOS_CSV:
            ruta_completa = os.path.join(current_dir, CARPETA_CSV, archivo)
            if not os.path.exists(ruta_completa):
                logger.error(f"No encuentro el archivo: {ruta_completa}")
                continue

            logger.info(f"--- Procesando {archivo} ---")
            
            try:
                # 1. Leer CSV
                df_csv = pd.read_csv(ruta_completa)
                
                # 2. Filtrar filas con error_type = 'SIN_DATOS' o con error_type no vacío
                if 'error_type' in df_csv.columns:
                    filas_antes_filtro = len(df_csv)
                    df_csv = df_csv[df_csv['error_type'].isna() | (df_csv['error_type'] == '')]
                    filas_despues_filtro = len(df_csv)
                    if filas_antes_filtro > filas_despues_filtro:
                        logger.info(f"Se filtraron {filas_antes_filtro - filas_despues_filtro} filas con error_type = 'SIN_DATOS'")
                
                if df_csv.empty:
                    logger.warning(f"El archivo {archivo} no tiene registros válidos después del filtrado.")
                    continue
                
                # 3. Verificar si el CSV ya tiene id_link_producto o necesita merge
                if 'id_link_producto' in df_csv.columns:
                    # El CSV ya tiene id_link_producto, validar que existan en la BD
                    logger.info("El CSV ya contiene id_link_producto, validando existencia en BD...")
                    ids_validos = set(df_links_db['id_link_producto'].unique())
                    df_csv = df_csv[df_csv['id_link_producto'].isin(ids_validos)]
                    df_merged = df_csv.copy()
                    
                    if df_merged.empty:
                        logger.warning(f"El archivo {archivo} no tiene id_link_producto válidos en la BD.")
                        continue
                else:
                    # Hacer merge para obtener IDs
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
                
                # 4. Obtener fecha del archivo
                # Intentar extraer fecha del nombre del archivo (formato: BACKUP_RAW_YYYYMMDD_*.csv)
                # Si no se encuentra, usar la fecha del CSV
                fecha_str = None
                import re
                match = re.search(r'(\d{4})(\d{2})(\d{2})', archivo)
                if match:
                    año, mes, dia = match.groups()
                    fecha_str = f"{año}-{mes}-{dia}"
                    logger.info(f"Fecha extraída del nombre del archivo: {fecha_str}")
                
                if not fecha_str:
                    # Fallback: usar fecha del CSV
                    fecha_str = df_merged['fecha'].iloc[0]
                    logger.info(f"Usando fecha del CSV: {fecha_str}")

                # --- CORRECCIÓN DEL ERROR DE DUPLICADOS ---
                # Si un producto está 2 veces en el CSV, nos quedamos con el primero.
                filas_antes = len(df_merged)
                df_merged.drop_duplicates(subset=['id_link_producto'], keep='first', inplace=True)
                filas_despues = len(df_merged)
                if filas_antes > filas_despues:
                    logger.warning(f"¡Atención! Se eliminaron {filas_antes - filas_despues} productos duplicados en el CSV.")

                # 4. Crear Extracción
                id_extraccion = self.crear_extraccion_manual(archivo, fecha_str)
                df_merged['id_extraccion'] = id_extraccion

                # 5. Mapeo y Limpieza
                rename_map = {'nombre': 'nombre_producto'}
                df_merged.rename(columns=rename_map, inplace=True)

                # Correcciones ortográficas
                logger.info("Aplicando correcciones ortográficas...")
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

                # Formato de fechas - usar la fecha extraída del nombre del archivo o del CSV
                df_merged['fecha_extraccion'] = pd.to_datetime(fecha_str).date()
                df_merged['created_at'] = datetime.now()

                # 6. Selección final de columnas
                cols_destino = [
                    'id_link_producto', 'id_extraccion', 'nombre_producto',
                    'precio_normal', 'precio_descuento', 'precio_por_unidad',
                    'unidad_medida', 'peso', 'created_at', 'fecha_extraccion'
                ]
                
                df_final = df_merged[cols_destino].copy()
                df_final['precio_descuento'] = df_final['precio_descuento'].fillna(0)

                # 7. Insertar
                logger.info(f"Insertando {len(df_final)} registros limpios...")
                df_final.to_sql(
                    'precios_productos', 
                    con=self.db.engine, 
                    if_exists='append', 
                    index=False
                )
                
                # Cerrar extracción con éxito
                self.cerrar_extraccion(id_extraccion, len(df_final), 'completada')
                logger.info("¡Éxito!")

            except Exception as e:
                logger.error(f"Error procesando {archivo}: {e}")
                # Si falla, intenta marcar la extracción como error si existe la variable
                if 'id_extraccion' in locals():
                     self.cerrar_extraccion(id_extraccion, 0, 'error')

        self.db.close_connections()

if __name__ == "__main__":
    cargador = CargaHistorica()
    cargador.procesar_y_cargar()