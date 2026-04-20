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
    
    def transform_alto_riesgo_caps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza y unifica columnas con lógica de fechas híbrida."""
        if df.empty: return df
        df = df.copy()
        
        # 1. Normalización inicial de nombres
        df.columns = [
            col.strip().lower()
            .replace(' ', '_').replace('°', '').replace('.', '').replace('/', '_')
            .replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
            for col in df.columns
        ]

        # 2. UNIFICACIÓN DE COLUMNAS (Mismo mapeo que tenías)
        mapeo_unificacion = {
            'fecha_de_derivacion': ['fecha_de_derivacio', 'f_de_derivacion'],
            'sem_gest': ['sema_gest', 'semana_gest', 'semana_gestacional'],
            'observaciones_rn': ['observaciones___rn', 'observaciones']
        }

        for principal, variantes in mapeo_unificacion.items():
            if principal not in df.columns:
                df[principal] = np.nan
            for var in variantes:
                if var in df.columns:
                    df[principal] = df[principal].fillna(df[var])
                    df = df.drop(columns=[var])

        df = df.loc[:, ~df.columns.duplicated()]

        # --- LÓGICA DE FECHAS INTELIGENTE ---
        def parsear_fecha_hibrida(val):
            val = str(val).strip().replace('#', '')
            if val.lower() in ['nan', 'none', '', 'sin dato']: return pd.NaT
            
            try:
                # Intentamos primero el formato estándar (Día/Mes/Año)
                # 'dayfirst=True' es vital para Corrientes
                return pd.to_datetime(val, dayfirst=True, errors='raise')
            except:
                try:
                    # Si falla (ej: 4/25/2026), probamos el formato yanqui (Mes/Día/Año)
                    return pd.to_datetime(val, dayfirst=False, errors='coerce')
                except:
                    return pd.NaT

        # Aplicamos a las columnas de fecha
        cols_fechas = ['fecha_de_notif', 'fecha_de_derivacion', 'f_de_nac']
        for col in cols_fechas:
            if col in df.columns:
                df[col] = df[col].apply(parsear_fecha_hibrida)

        # 4. Conversión de NUMÉRICOS
        cols_int = ['n_orden', 'caps', 'dni']
        for col in cols_int:
            if col in df.columns:
                s = df[col].astype(str).str.replace('.', '', regex=False).str.strip()
                df[col] = pd.to_numeric(s, errors='coerce').fillna(0).astype('int64')

        # 5. Selección y Limpieza de STRINGS
        columnas_finales = [
            'n_orden', 'fecha_de_notif', 'fecha_de_derivacion', 'caps', 
            'nombre_y_apellido', 'dni', 'f_de_nac', 'sem_gest', 
            'fum_fpp', 'fp_tpo', 'motivo_de_derivacion', 'observaciones_rn', 
            'anticoncepcion', 'seguimiento', 'hospital_origen'
        ]
        
        for col in columnas_finales:
            if col not in df.columns:
                df[col] = "SIN DATO"

        df = df[columnas_finales]

        for col in df.columns:
            if df[col].dtype == 'object' or df[col].dtype.name == 'object':
                df[col] = df[col].astype(str).str.upper().str.strip().replace(['NAN', 'NONE', 'NAT', ''], 'SIN DATO')

        logger.info("[TRANSFORM] Alto Riesgo CAPS: %d columnas listas (fechas corregidas).", len(df.columns))
        return df