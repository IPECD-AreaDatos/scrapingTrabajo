import os
import logging
import pandas as pd
from json import loads
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

class Extract:
    def __init__(self):
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
        self.creds = service_account.Credentials.from_service_account_info(self.key_dict, scopes=self.scopes)
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.sheet = self.service.spreadsheets()

    def extract_derivaciones(self, spreadsheet_id, meses_map) -> pd.DataFrame:
        """Extrae datos de la Red de Obstetricia (Multi-hoja)."""
        logger.info("[EXTRACT] Extrayendo Derivaciones Red Obstetricia...")

        all_dfs = []

        for mes_nombre, fecha_valor in meses_map.items():
            try:
                rango = f"'{mes_nombre}'!A:O"
                result = self.sheet.values().get(spreadsheetId=spreadsheet_id, range=rango).execute()
                values = result.get('values', [])

                if not values:
                    logger.warning(f"[EXTRACT] Hoja {mes_nombre} vacía.")
                    continue

                # Crear DataFrame de la hoja actual
                headers = values[0]
                df_mes = pd.DataFrame(values[1:], columns=headers)
                
                # Limpieza rápida de filas vacías antes de juntar
                df_mes = df_mes[df_mes['DNI'].notna() & (df_mes['DNI'] != '')]
                
                # AGREGAMOS LA COLUMNA MÁGICA
                df_mes['fecha_proceso_mes'] = fecha_valor
                df_mes['nombre_mes_tab'] = mes_nombre
                
                all_dfs.append(df_mes)
                logger.info(f"[EXTRACT] {mes_nombre}: {len(df_mes)} filas leídas.")

            except Exception as e:
                # Si una hoja no existe todavía (ej. MAYO), que no rompa todo el script
                logger.error(f"[EXTRACT] Error leyendo {mes_nombre}: {e}")

        if not all_dfs:
            return pd.DataFrame()

        # Concatenamos todo en un solo DF
        df_final = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"[EXTRACT] TOTAL ACUMULADO: {len(df_final)} filas.")
        
        return df_final
    
    def extract_sumar(self, spreadsheet_id, fecha_proceso) -> pd.DataFrame:
        """Extrae datos del Padrón SUMAR (Hoja única)."""
        logger.info(f"[EXTRACT] Extrayendo Padrón SUMAR (Mes: {fecha_proceso})...")

        rango = "Embarazadas!A:AQ" 
        result = self.sheet.values().get(spreadsheetId=spreadsheet_id, range=rango).execute()
        values = result.get('values', [])
        
        if not values: return pd.DataFrame()
        
        df = pd.DataFrame(values[1:], columns=values[0])

        df['fecha_proceso_sumar'] = fecha_proceso
        
        logger.info(f"[EXTRACT] SUMAR: {len(df)} filas leídas.")
        return df
    
    def extract_alto_riesgo_caps(self, spreadsheet_id) -> pd.DataFrame:
        logger.info("[EXTRACT] Leyendo Embarazadas Derivadas de Alto Riesgo CAPS...")
        hojas = ["HOSPITAL VIDAL", "HOSPITAL LLANO"]
        all_dfs = []

        for hoja in hojas:
            try:
                # Traemos un rango amplio para no perder nada
                rango = f"'{hoja}'!A1:Z100" 
                result = self.sheet.values().get(spreadsheetId=spreadsheet_id, range=rango).execute()
                values = result.get('values', [])

                if not values: continue

                # BUSCAMOS LA FILA DE ENCABEZADOS (la que tiene 'DNI' o 'N° orden')
                header_idx = 0
                for i, row in enumerate(values):
                    # Buscamos 'DNI' en cualquier parte de la fila i
                    if any('DNI' in str(cell).upper() for cell in row):
                        header_idx = i
                        break
                
                headers = values[header_idx]
                data = values[header_idx + 1:]

                # Limpiamos headers (quitamos espacios y saltos de línea)
                headers = [str(h).strip().replace('\n', ' ') for h in headers]

                # Creamos el DF. Si hay discrepancia de columnas, usamos solo las que coinciden
                df_temp = pd.DataFrame(data)
                
                # Cortamos o rellenamos columnas para que coincidan con headers
                df_temp = df_temp.iloc[:, :len(headers)]
                df_temp.columns = headers

                # Normalizamos nombres para filtrar
                df_temp.columns = [c.upper() for c in df_temp.columns]
                
                # Filtramos: Solo filas donde DNI tenga algo
                if 'DNI' in df_temp.columns:
                    df_temp = df_temp[df_temp['DNI'].notna() & (df_temp['DNI'] != '')]
                
                df_temp['hospital_origen'] = hoja
                all_dfs.append(df_temp)
                logger.info(f"[EXTRACT] {hoja}: {len(df_temp)} filas obtenidas.")

            except Exception as e:
                logger.error(f"[EXTRACT] Error crítico en hoja {hoja}: {e}")

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()