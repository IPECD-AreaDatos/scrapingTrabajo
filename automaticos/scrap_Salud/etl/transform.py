import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class Transform:
    def _normalizar_columnas(self, df):
        df.columns = [col.strip().lower().replace(' ', '_').replace('.', '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u') for col in df.columns]
        return df

    def _to_date(self, series):
        """Convierte a fecha manejando el formato DD/MM/YYYY típico del SUMAR."""
        return pd.to_datetime(series, dayfirst=True, errors='coerce')
    
    def transform_derivaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lógica específica para Red Obstetricia."""
        if df.empty:
            return df
            
        logger.info("[TRANSFORM] Iniciando normalización de %d filas...", len(df))
        df = df.copy()
        df = self._normalizar_columnas(df)

        # --- UNIFICACIÓN DE VARIANTES DE FECHA NACIMIENTO ---
        # Definimos las variantes que encontramos (puedes agregar más si aparecen)
        variantes_nacimiento = ['fecha_de_nacimiento', 'fecha_nacimienot', 'f_nacimiento']
        
        if 'fecha_nacimiento' not in df.columns:
            # Si la principal no existe, creamos una vacía para empezar a llenar
            df['fecha_nacimiento'] = np.nan

        for var in variantes_nacimiento:
            if var in df.columns:
                logger.info(f"[TRANSFORM] Unificando columna '{var}' en 'fecha_nacimiento'")
                # Llenamos los nulos de la principal con los datos de la variante
                df['fecha_nacimiento'] = df['fecha_nacimiento'].fillna(df[var])
                # Borramos la columna variante para que no sobre
                df = df.drop(columns=[var])

        # Eliminamos duplicados exactos por si quedó alguno
        df = df.loc[:, ~df.columns.duplicated()]

        # 2. MANEJO DE FECHA POR MES (Construcción manual)
        def construir_fecha(row):
            # 1. Limpieza básica
            val = str(row['fecha']).strip().lower()
            if val in ['nan', 'none', '', 'no valido']: return pd.NaT
            
            try:
                # 2. Caso: Número serial de Sheets (ej: 46056)
                if val.replace('.0', '').isdigit() and len(val.replace('.0', '')) >= 5:
                    return pd.to_datetime(int(float(val)), unit='D', origin='1899-12-30')

                # 3. Mapeo de meses para textos como "23-feb" o "01-mar"
                meses_es = {
                    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
                }

                # Reemplazamos guiones por barras para unificar
                val_limpio = val.replace('-', '/')
                
                # Buscamos si hay algún nombre de mes en el texto
                for nombre, num in meses_es.items():
                    if nombre in val_limpio:
                        # Extraemos el día (los dígitos que estén antes o después)
                        dia = int(''.join(filter(str.isdigit, val_limpio)))
                        return pd.Timestamp(year=2026, month=num, day=dia)

                # 4. Caso: Formato con barras (día/mes o mes/día según el mes)
                partes = val_limpio.split('/')
                if len(partes) >= 2:
                    p1 = int(''.join(filter(str.isdigit, partes[0])))
                    p2 = int(''.join(filter(str.isdigit, partes[1])))
                    
                    if row.get('nombre_mes_tab') == 'ABRIL':
                        mes, dia = p1, p2
                    else:
                        dia, mes = p1, p2
                    
                    return pd.Timestamp(year=2026, month=mes, day=dia)

            except Exception as e:
                logger.debug(f"[TRANSFORM] Error parseando fecha '{val}': {e}")
            
            # 5. Intento final con el parseador de pandas
            return pd.to_datetime(val, dayfirst=True, errors='coerce')

        logger.info("[TRANSFORM] Reconstruyendo fechas (incluyendo formatos con guiones)...")
        df['fecha'] = df.apply(construir_fecha, axis=1)
        
        # Fecha de Nacimiento: suele ser más larga (trae el año), 
        df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce')

        # 3. Conversión a NUMÉRICOS (INT)
        cols_int = ['region_sanitaria', 'dni', 'edad_embarazada', 'cantidad_controles']
        for col in cols_int:
            if col in df.columns:
                s = df[col].astype(str).str.replace('.', '', regex=False).str.strip()
                df[col] = pd.to_numeric(s, errors='coerce').fillna(0).astype(int)

        # 4. Limpieza de STRINGS
        cols_str = [
            'efector_que_deriva', 'medico_derivador', 'medico_receptor', 
            'maternidad_que_recibe', 'apellido_y_nombre', 'edad_gestacional', 'nro_de_gestas', 
            'diagnostico_de_derivacion_materna', 'primer_control'
        ]
        for col in cols_str:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()
                df[col] = df[col].replace(['NAN', 'NONE', 'NAT', ''], 'SIN DATO')

        # 6. Limpieza final de columnas sobrantes
        cols_a_eliminar = ['fecha_proceso_mes', 'nombre_mes_tab']
        df = df.drop(columns=[c for c in cols_a_eliminar if c in df.columns])

        logger.info("[TRANSFORM] Reporte Final: %d filas y %d columnas.", len(df), len(df.columns))
        return df
    
    def transform_sumar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lógica específica para Padrón SUMAR."""
        if df.empty: return df

        logger.info("[TRANSFORM] Iniciando transformación de Padrón SUMAR (%d filas)...", len(df))
        df = df.copy()

        # 1. Normalizar solo formato de nombres (MANTIENE EL 'AFI')
        df = self._normalizar_columnas(df)

        # 2. Conversión de FECHAS (usamos nombres con 'afi' o como vengan en minúsculas)
        cols_fechas = [
            'afifechanac', 'fechadiagnosticoembarazo', 'fechaprobableparto', 
            'fechaultimaprestacion', 'fum', 'fechainiciosegundiag', 'fecha_prest', 'fecha_proceso_sumar'
        ]
        for col in cols_fechas:
            if col in df.columns:
                df[col] = self._to_date(df[col])

        # 3. Conversión de NUMÉRICOS
        cols_int = [
            'clavebeneficiario', 'afidni', 'semanasembarazo', 'afidomnro', 
            'afitelefono', 'dependenciasanit', 'codigoprovincialefector', 'edad'
        ]
        for col in cols_int:
            if col in df.columns:
                # Aseguramos que sea Serie por si quedó algún duplicado
                serie = df[col].iloc[:, 0] if isinstance(df[col], pd.DataFrame) else df[col]
                s = serie.astype(str).str.replace(r'\D', '', regex=True)
                df[col] = pd.to_numeric(s, errors='coerce').fillna(0).astype('int64')

        # 4. Limpieza de STRINGS
        cols_obj = df.select_dtypes(include=['object']).columns
        for col in cols_obj:
            serie_str = df[col].iloc[:, 0] if isinstance(df[col], pd.DataFrame) else df[col]
            df[col] = serie_str.astype(str).str.upper().str.strip().replace(['NAN', 'NONE', 'NAT', '', 'SIN DATO'], 'SIN DATO')

        # 5. Domicilio Unificado (respetando nombres afi)
        #if 'afidomcalle' in df.columns and 'afidomnro' in df.columns:
        #    df['domicilio_completo'] = df['afidomcalle'].astype(str) + " " + df['afidomnro'].astype(str)
        #    df['domicilio_completo'] = df['domicilio_completo'].str.replace('SIN DATO 0', 'SIN DATO')

        logger.info("[TRANSFORM] SUMAR: %d columnas listas.", len(df.columns))
        return df