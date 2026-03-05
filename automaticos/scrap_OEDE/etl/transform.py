"""
TRANSFORM - Módulo de transformación de datos OEDE
Responsabilidad: Construir el DataFrame consolidado de todas las provincias
"""
import os
import logging
import pandas as pd
from unidecode import unidecode
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'ev_remun_trab_reg_por_sector.xlsx'

PROVINCIAS = {
    'Partidos de GBA': 1, 
    'Capital Federal': 2, 
    'Resto de Buenos Aires': 6, 
    'Catamarca': 10,
    'Cordoba': 14, 
    'Corrientes': 18, 
    'Chaco': 22, 
    'Chubut': 26,
    'Entre Rios': 30, 
    'Formosa': 34, 
    'Jujuy': 38, 
    'La Pampa': 42,
    'La Rioja': 46, 
    'Mendoza': 50, 
    'Misiones': 54, 
    'Neuquen': 58,
    'Rio Negro': 62, 
    'Salta': 66, 
    'San Juan': 70, 
    'San Luis': 74,
    'Santa Cruz': 78, 
    'Santa Fe': 82, 
    'Santiago del Estero': 86,
    'Tierra del Fuego': 90, 
    'Tucuman': 94
}


class TransformOEDE:
    """Transforma el Excel de OEDE en un DataFrame consolidado por provincia."""

    def __init__(self, host, user, password, database, port, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = self._crear_engine()

    def _crear_engine(self):
        # Si port es None o 'None', asignamos el puerto por defecto según el motor
        if not self.port or self.port == "None":
            puerto = 3306 if self.version == "1" else 5432
        else:
            puerto = int(self.port)

        if self.version == "1":
            url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
        else:
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
        
        return create_engine(url)

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        """
        Construye el DataFrame final con datos de todas las provincias.

        Returns:
            pd.DataFrame con columnas [fecha, id_provincia, id_categoria, id_subcategoria, valor]
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Construyendo diccionario desde BD...")
        self._construir_dic()

        logger.info("[TRANSFORM] Procesando %d provincias...", len(PROVINCIAS))
        dfs = []
        for provincia, id_prov in PROVINCIAS.items():
            logger.info("[TRANSFORM] Procesando: %s (id=%d)", provincia, id_prov)
            df = self._construir_df(ruta_archivo, provincia, id_prov)
            dfs.append(df)

        df_final = pd.concat(dfs, ignore_index=True)
        logger.info("[TRANSFORM] DataFrame final: %d filas", len(df_final))
        return df_final

    def _construir_dic(self):
        tabla = pd.read_sql_query("SELECT * FROM dicc_oede", con=self.engine)
        diccionario = {}
        for _, row in tabla.iterrows():
            clave = self._formatear_key(row['nombre'])
            valor = [row['id_categoria'], int(row['id_subcategoria'])]
            diccionario.setdefault(clave, []).append(valor)
        self._diccionario = diccionario

    def _construir_df(self, ruta_archivo: str, provincia: str, id_prov: int) -> pd.DataFrame:
        # 1. Leer el Excel
        df = pd.read_excel(ruta_archivo, sheet_name=provincia, skiprows=3)
        
        # 2. Limpieza de columnas iniciales
        df = df.drop(df.columns[0], axis=1) # Elimina 'Unnamed: 0'
        df.rename(columns={df.columns[0]: 'claves_listas'}, inplace=True)

        # 3. Mapeo de categorías e IDs
        df = df.dropna(subset=['claves_listas'])
        df['claves_listas_limpias'] = df['claves_listas'].astype(str).apply(self._formatear_key)

        df[['id_categoria', 'id_subcategoria']] = df.apply(
            lambda row: pd.Series(self._mapear_clave(row['claves_listas_limpias'], row.name)),
            axis=1
        )

        # 4. Seleccionar columnas de tiempo
        # CAMBIO: Ahora buscamos la palabra 'Trim' en el nombre de la columna
        columnas_tiempo = [c for c in df.columns if 'Trim' in str(c)]
        cols_finales = ['id_categoria', 'id_subcategoria'] + columnas_tiempo
        df_reducido = df[cols_finales]

        # 5. MELT
        df_t = pd.melt(
            df_reducido,
            id_vars=['id_categoria', 'id_subcategoria'],
            var_name='trimestre_raw',
            value_name='valor'
        )

        # 6. Función para convertir "1º Trim 1996" a "1996-01-01"
        def parse_trimestre_texto(x):
            try:
                txt = str(x).lower()
                # Extraer el año (últimos 4 caracteres)
                ano = txt[-4:]
                # Mapear el inicio del trimestre
                mes = '01' # Por defecto
                if '1' in txt: mes = '01'
                elif '2' in txt: mes = '04'
                elif '3' in txt: mes = '07'
                elif '4' in txt: mes = '10'
                
                return pd.to_datetime(f"{ano}-{mes}-01")
            except:
                return pd.NaT

        df_t['fecha'] = df_t['trimestre_raw'].apply(parse_trimestre_texto)
        
        # 7. Limpieza y formato final para Postgres
        df_t = df_t.dropna(subset=['fecha'])
        df_t['valor'] = pd.to_numeric(df_t['valor'], errors='coerce')
        df_t['id_provincia'] = id_prov

        # Retornamos solo las columnas necesarias
        return df_t[['fecha', 'id_provincia', 'id_categoria', 'id_subcategoria', 'valor']]
    
    def _mapear_clave(self, clave: str, index: int) -> list:
        if 'calzado' in clave or 'cuero' in clave:
            return ['D', 19]
        if 'edicion' in clave:
            return ['D', 22]
        valores = self._diccionario.get(clave, [[None, None]])
        if len(valores) > 1:
            return valores[1] if index >= 40 else valores[0]
        return valores[0]

    @staticmethod
    def _formatear_key(key: str) -> str:
        if pd.isna(key) or not isinstance(key, (str, bytes)):
            return ""
        return unidecode(str(key).lower()).replace(',', '').replace('.', '').replace(' ', '')

    def close(self):
        if self.engine:
            self.engine.dispose()
