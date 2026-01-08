import sys
import os
import pandas as pd
import logging
import re
from datetime import datetime
from dotenv import load_dotenv

# Configurar paths para importar utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_sheets import ConexionGoogleSheets
from utils.utils_db import ConexionBaseDatos

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class CargadorLinksDB:
    def __init__(self):
        # 1. Conexión a Sheets
        SHEET_ID = '13vz5WzXnXLdp61YVHkKO17C4OBEXVJcC5cP38N--8XA' 
        self.gs = ConexionGoogleSheets(SHEET_ID)
        
        # 2. Conexión a Base de Datos (Usando tus variables de entorno)
        self.db = ConexionBaseDatos(
            host=os.getenv('HOST_DBB'),
            user=os.getenv('USER_DBB'),
            password=os.getenv('PASSWORD_DBB'),
            database='canasta_basica_supermercados' # Nombre exacto de tu base
        )
        
        # Diccionarios para memoria caché (Nombre -> ID)
        self.mapa_categorias = {} 
        self.mapa_supermercados = {}

        self.traduccion_supers = {
            'delimart': 'Delimart',
            'carrefour': 'Carrefour',
            'masonline': 'MasOnline',
            'paradacanga': 'Parada Canga',  # Aquí arreglamos el espacio
            'depot': 'Depot',
            'lareina': 'La Reina',          # Aquí arreglamos el espacio
            'dia': 'DIA%'                   # Aquí arreglamos el símbolo y mayúsculas
        }

    def cargar_mapas_ids(self):
        """Descarga IDs de la base de datos"""
        if not self.db.connect_db():
            raise Exception("No se pudo conectar a la BD")

        # 1. Cargar Supermercados (Mapeo: Nombre Exacto -> ID)
        supers = self.db.execute_query("SELECT id_super, nombre FROM supermercados")
        # Nota: Ya NO usamos .lower() aquí para ser precisos con tu diccionario
        self.mapa_supermercados = {nombre: id_sup for id_sup, nombre in supers}
        
        # 2. Cargar Categorías (Mapeo: Nombre Lower -> ID)
        # Las categorías sí las dejamos con lower() y strip() para ser flexibles
        cats = self.db.execute_query("SELECT id_categoria, nombre FROM categorias")
        self.mapa_categorias = {nombre.lower().strip(): id_cat for id_cat, nombre in cats}
        
        logger.info(f"Mapas cargados: {len(self.mapa_supermercados)} supers, {len(self.mapa_categorias)} categorías.")
        self.db.close_connections()

    def _limpiar_numero(self, val):
        """Limpia precios/pesos (mismo que tu extractor)"""
        if pd.isna(val): return None
        s = str(val).strip()
        s = re.sub(r'[^\d,.]', '', s).replace(',', '.')
        try:
            return float(s)
        except:
            return None

    def procesar_hoja(self, nombre_hoja_excel):
        """Procesa una hoja específica del Excel usando el nombre corregido"""
        
        # 1. TRADUCIR EL NOMBRE (Excel -> Base de Datos)
        nombre_real_db = self.traduccion_supers.get(nombre_hoja_excel)
        
        if not nombre_real_db:
            logger.error(f"No existe traducción configurada para la hoja '{nombre_hoja_excel}'")
            return

        # 2. BUSCAR EL ID USANDO EL NOMBRE REAL
        id_super = self.mapa_supermercados.get(nombre_real_db)
        
        if not id_super:
            logger.error(f"El super '{nombre_real_db}' (hoja: {nombre_hoja_excel}) no se encontró en la DB. IDs cargados: {self.mapa_supermercados}")
            return

        logger.info(f"Procesando hoja: {nombre_hoja_excel} -> DB: {nombre_real_db} (ID: {id_super})")

        # -------------------------------------------------------------
        # 3. LEER SHEETS Y ARMAR DATOS (Esto es lo que faltaba)
        # -------------------------------------------------------------
        try:
            # Leemos el Excel usando el nombre de la hoja original
            df = self.gs.leer_df(f"'{nombre_hoja_excel}'!A2:D1000", header=False)
        except Exception as e:
            logger.error(f"Error leyendo hoja {nombre_hoja_excel}: {e}")
            return

        registros_para_insertar = []
        
        for idx, row in df.iterrows():
            # Validaciones básicas
            if len(row) < 2 or pd.isna(row[0]): continue
            
            nombre_categoria_excel = str(row[0]).strip()
            link = str(row[1]).strip()
            
            if not link.startswith(('http', 'www')): continue

            # BUSCAR ID DE CATEGORÍA
            id_cat = self.mapa_categorias.get(nombre_categoria_excel.lower())
            
            if not id_cat:
                logger.warning(f"Categoría desconocida en Excel: '{nombre_categoria_excel}'. Saltando link: {link}")
                continue

            peso = self._limpiar_numero(row[2]) if len(row) > 2 else None
            unidad = str(row[3]).strip().upper() if len(row) > 3 and pd.notna(row[3]) else None

            # Armar tupla para SQL
            # (id_categoria, id_supermercado, link, peso, unidad_medida, activo, created_at, updated_at)
            registros_para_insertar.append((
                id_cat,
                id_super,
                link,
                peso,
                unidad
            ))

        # 4. INSERTAR EN BASE DE DATOS
        if registros_para_insertar:
            self.insertar_lote(registros_para_insertar)
        else:
            logger.warning(f"No se encontraron links válidos para {nombre_hoja_excel}")

    def insertar_lote(self, datos):
        """Realiza el INSERT masivo"""
        if not self.db.connect_db(): return

        query = """
        INSERT INTO link_productos 
        (id_categoria, id_supermercado, link, peso, unidad_medida, activo, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, 1, NOW(), NOW())
        ON DUPLICATE KEY UPDATE 
            id_categoria = VALUES(id_categoria),
            peso = VALUES(peso),
            unidad_medida = VALUES(unidad_medida),
            activo = 1,
            updated_at = NOW();
        """
        
        try:
            with self.db.connection.cursor() as cursor:
                cursor.executemany(query, datos)
                self.db.connection.commit()
                logger.info(f" {cursor.rowcount} registros procesados (insertados/actualizados).")
        except Exception as e:
            logger.error(f"Error SQL: {e}")
        finally:
            self.db.close_connections()

    def ejecutar_todo(self):
        self.cargar_mapas_ids()
        
        # Lista de hojas a procesar
        hojas = ['delimart', 'carrefour', 'masonline', 'paradacanga', 'depot', 'lareina', 'dia']

        #hojas = [ 'paradacanga',  'lareina', 'dia']
        
        for hoja in hojas:
            self.procesar_hoja(hoja)

if __name__ == "__main__":
    loader = CargadorLinksDB()
    loader.ejecutar_todo()