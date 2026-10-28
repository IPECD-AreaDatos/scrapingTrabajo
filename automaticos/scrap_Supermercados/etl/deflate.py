"""
DEFLATE - Módulo de deflactación de datos Supermercados
Responsabilidad: Calcular valores reales usando el IPC y actualizar Google Sheets.
"""
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

class DeflateSupermercados:
    def __init__(self, host, user, password, db_datalake, db_dwh, port=None, version="2"):
        self.host, self.user, self.password = host, user, password
        self.db_datalake = db_datalake  # De donde lee IPC
        self.db_dwh = db_dwh            # Donde escribe resultado
        self.port = port
        self.version = str(version)

    def _crear_engine(self, database):
        """Crea un motor para una base de datos específica según la versión"""
        if self.version == "1":
            # MySQL
            puerto = int(self.port) if self.port else 3306
            url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{database}"
        else:
            # PostgreSQL
            puerto = int(self.port) if self.port else 5432
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{database}"
        
        from sqlalchemy import create_engine # Asegúrate de tener el import
        return create_engine(url)

    def run(self, df_super):
        logger.info("[DEFLATE] Iniciando proceso de deflactación...")
        
        # 1. Obtenemos el IPC del DATALAKE
        fecha_min, fecha_max = df_super['fecha'].min(), df_super['fecha'].max()
        df_ipc = self._get_data_ipc(fecha_min, fecha_max)
        
        # 2. Procesamos (igual que antes)
        df_merged = pd.merge(df_super, df_ipc, on=['fecha', 'id_region'], how='left')
        df_deflactado = self._calcular_deflactacion(df_merged)
        
        # 3. Cargamos al DWH
        self._cargar_bdd(df_deflactado)
        
        # 4. Sheets
        self._update_sheets(df_deflactado)

    def _get_data_ipc(self, fmin, fmax):
        # Usamos un engine temporal conectado al DATALAKE
        engine_lake = self._crear_engine(self.db_datalake)
        ids_necesarios = [1, 13, 15, 4, 6, 5, 9, 3, 7, 10, 26, 45, 17, 38]
        
        query = text("""
            SELECT fecha, id_region as id_region, id_subdivision, valor 
            FROM ipc 
            WHERE fecha BETWEEN :fmin AND :fmax 
            AND id_subdivision IN :ids
        """)
        
        try:
            with engine_lake.connect() as conn:
                df_raw = pd.read_sql(query, conn, params={"fmin": fmin, "fmax": fmax, "ids": tuple(ids_necesarios)})
            
            df_pivot = df_raw.pivot_table(index=['fecha', 'id_region'], 
                                         columns='id_subdivision', 
                                         values='valor').reset_index()

            # La apertura 38 (equipos audiovisuales) solo está publicada para GBA.
            # La serie histórica de supermercados usaba ese índice como proxy para
            # las demás regiones; conservarla evita dejar electrónica a valores nominales.
            df_electronica_gba = df_raw[
                (df_raw['id_region'] == 2) & (df_raw['id_subdivision'] == 38)
            ][['fecha', 'valor']].rename(columns={'valor': 'elec_gba'})
            df_pivot = df_pivot.merge(df_electronica_gba, on='fecha', how='left')
            
            nombres = {
                1: 'gen', 13: 'aguas', 15: 'alc', 4: 'pan', 6: 'lact', 5: 'carn', 
                9: 'verd', 3: 'alim', 7: 'aceit', 10: 'azu', 26: 'limp', 45: 'pers', 17: 'ropa', 38: 'elec'
            }
            return df_pivot.rename(columns=nombres)
        finally:
            engine_lake.dispose()

    def _calcular_deflactacion(self, df):
        logger.info("[DEFLATE] Calculando valores reales...")
        df_d = pd.DataFrame()
        # Mantenemos las columnas de identificación
        df_d['fecha'] = df['fecha']
        df_d['id_region'] = df['id_region']
        df_d['id_provincia'] = df['id_provincia']

        # Un índice faltante no equivale a IPC=100: eso dejaba la venta nominal.
        # Para Nación, las aperturas no disponibles usan el IPC general nacional.
        gen = pd.to_numeric(df['gen'], errors='coerce')
        if gen.isna().any() or (gen <= 0).any():
            faltantes = int((gen.isna() | (gen <= 0)).sum())
            raise ValueError(
                f"IPC general faltante o inválido para {faltantes} filas; "
                "se cancela la actualización para preservar la tabla actual."
            )

        es_nacion = df['id_region'].eq(1)

        def indice_con_respaldo(columna, respaldo=None):
            """Usa la apertura disponible, luego un respaldo y finalmente IPC general nacional."""
            if columna in df:
                indice = pd.to_numeric(df[columna], errors='coerce')
            else:
                indice = pd.Series(float('nan'), index=df.index)

            if respaldo is not None:
                indice = indice.fillna(respaldo)

            # En Nación faltan aperturas desagregadas en la fuente IPC; deflactar
            # con el índice general es preferible a dejar esos rubros nominales.
            indice = indice.mask(indice.isna() & es_nacion, gen)

            invalido = indice.isna() | (indice <= 0)
            if invalido.any():
                filas = int(invalido.sum())
                raise ValueError(
                    f"IPC {columna} faltante o inválido para {filas} filas "
                    "fuera de Nación; se cancela la actualización."
                )
            return indice

        aguas = indice_con_respaldo('aguas')
        alcohol = indice_con_respaldo('alc')
        pan = indice_con_respaldo('pan')
        aceite = indice_con_respaldo('aceit')
        azucar = indice_con_respaldo('azu')
        lacteos = indice_con_respaldo('lact')
        carnes = indice_con_respaldo('carn')
        verduras = indice_con_respaldo('verd')
        alimentos = indice_con_respaldo('alim')
        limpieza = indice_con_respaldo('limp')
        personal = indice_con_respaldo('pers')
        ropa = indice_con_respaldo('ropa')

        # IPC no publica la apertura 38 para las demás regiones: usar la serie GBA,
        # como hacía la deflactación histórica. Nación usa su índice general si falta.
        respaldo_electronica = (
            pd.to_numeric(df['elec_gba'], errors='coerce').mask(es_nacion)
            if 'elec_gba' in df else None
        )
        electronica = indice_con_respaldo('elec', respaldo=respaldo_electronica)
        
        df_d['total_facturacion'] = df['total_facturacion'] / (gen / 100)
        df_d['bebidas'] = df['bebidas'] / (((aguas * 0.5) + (alcohol * 0.5)) / 100)
        df_d['almacen'] = df['almacen'] / (((pan * 0.33) + (aceite * 0.33) + (azucar * 0.33)) / 100)
        df_d['panaderia'] = df['panaderia'] / (pan / 100)
        df_d['lacteos'] = df['lacteos'] / (lacteos / 100)
        df_d['carnes'] = df['carnes'] / (carnes / 100)
        df_d['verduleria_fruteria'] = df['verduleria_fruteria'] / (verduras / 100)
        
        # Rostisería usa Restaurantes (id 31 o similar, chequear si 'alim' es correcto)
        df_d['alimentos_preparados_rostiseria'] = df['alimentos_preparados_rostiseria'] / (alimentos / 100)
        
        df_d['articulos_limpieza_perfumeria'] = df['articulos_limpieza_perfumeria'] / (((limpieza * 0.5) + (personal * 0.5)) / 100)
        df_d['indumentaria_calzado_textiles_hogar'] = df['indumentaria_calzado_textiles_hogar'] / (ropa / 100)
        df_d['electronica_hogar'] = df['electronica_hogar'] / (electronica / 100)
        df_d['otros'] = df['otros'] / (gen / 100)
        
        # Reemplazar NaN por 0 para que Postgres no se queje si la columna es NOT NULL
        return df_d.round(2).fillna(0)

    def _cargar_bdd(self, df):
        # Usamos un engine temporal conectado al DWH
        engine_dwh = self._crear_engine(self.db_dwh)
        tabla = "supermercado_deflactado"
        schema = "public" if self.version == "2" else None
        full_table = f"{schema}.{tabla}" if schema else tabla
        
        try:
            with engine_dwh.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {full_table}"))
                df.to_sql(name=tabla, con=conn, schema=schema, if_exists='append', index=False)
            logger.info(f"[LOAD] Tabla {tabla} en {self.db_dwh} actualizada.")
        finally:
            engine_dwh.dispose()

    def _update_sheets(self, df):
        try:

            df['fecha'] = pd.to_datetime(df['fecha'])
            # Filtramos Corrientes y fecha desde 2018-12 (según tu lógica)
            df_corr = df[(df['id_provincia'] == 18) & (df['fecha'] >= pd.Timestamp('2018-12-01'))]
            df_corr = df_corr.sort_values('fecha')
            lista_valores = [df_corr['total_facturacion'].tolist()]

            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
            service = build('sheets', 'v4', credentials=creds)

            # Escribimos en la fila 11 (C11 en adelante)
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range='Datos!C11',
                valueInputOption='RAW',
                body={'values': lista_valores}
            ).execute()
            logger.info("[OK] Google Sheets actualizado con valores deflactados.")
        except Exception as e:
            logger.error(f"[ERROR SHEETS] {e}")
