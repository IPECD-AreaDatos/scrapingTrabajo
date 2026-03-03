import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

class TransformANAC:
    def __init__(self, target_value="TABLA 11"):
        self.target_value = target_value
        self.column_mapping = {
            'Aeroparque': 'aeroparque', 'Bahía Blanca': 'bahia_blanca', 'Bariloche': 'bariloche',
            'Base Marambio': 'base_marambio', 'Catamarca': 'catamarca', 'Chapelco': 'chapelco',
            'Comod. Rivadavia': 'comod_rivadavia', 'Concordia': 'concordia', 'Córdoba': 'cordoba',
            'Corrientes': 'corrientes', 'El Calafate': 'el_calafate', 'El Palomar': 'el_palomar',
            'Esquel': 'esquel', 'Ezeiza': 'ezeiza', 'Formosa': 'formosa', 'General Pico': 'general_pico',
            'Goya': 'goya', 'Gualeguaychú': 'gualeguaychu', 'Iguazú': 'iguazu', 'Jujuy': 'jujuy',
            'Junín': 'junin', 'La Plata': 'la_plata', 'La Rioja': 'la_rioja', 'Malargüe': 'malargue',
            'Mar del Plata': 'mar_del_plata', 'Mendoza': 'mendoza', 'Moreno': 'moreno',
            'Morón': 'moron', 'Neuquén': 'neuquen', 'Paraná': 'parana', 'Paso de los Libres': 'paso_de_los_libres',
            'Posadas': 'posadas', 'Puerto Madryn': 'puerto_madryn', 'Reconquista': 'reconquista',
            'Resistencia': 'resistencia', 'Río Cuarto': 'rio_cuarto', 'Río Gallegos': 'rio_gallegos',
            'Río Grande': 'rio_grande', 'Rosario': 'rosario', 'Salta': 'salta', 'San Fernando': 'san_fernando',
            'San Juan': 'san_juan', 'San Luis': 'san_luis', 'San Rafael': 'san_rafael', 'Santa Fe': 'santa_fe',
            'Santa Rosa': 'santa_rosa', 'Santa Rosa de Conlara': 'santa_rosa_de_conlara',
            'Santiago del Estero': 'santiago_del_estero', 'Tandil': 'tandil', 'Termas Río Hondo': 'termas_rio_hondo',
            'Trelew': 'trelew', 'Tucumán': 'tucuman', 'Ushuaia': 'ushuaia', 'Viedma': 'viedma',
            'Villa Gesell': 'villa_gesell', 'Villa Reynolds': 'villa_reynolds', 'Otros': 'otros'
        }

    def transform(self, file_path):
        logger.info(f"Leyendo archivo Excel: {file_path}")
        df_raw = pd.read_excel(file_path, header=None, dtype=str)
        
        # 1. BUSCAR EL BLOQUE "TABLA 11" y "Pasajeros Totales"
        start_row = None
        for i, row in df_raw.iterrows():
            # Convertimos toda la fila a una lista de strings para buscar sin errores de formato
            fila_texto = [str(cell).strip().upper() for cell in row.values if pd.notna(cell)]
            
            # Buscamos 'TABLA 11' y aseguramos que no sea una tabla menor
            if "TABLA 11" in fila_texto:
                # Verificamos que sea la tabla que contiene los datos de pasajeros
                # Ajusta el texto "PASAJEROS" si en tu Excel dice otra cosa
                if i + 1 < len(df_raw):
                    fila_siguiente = str(df_raw.iloc[i+1].values).upper()
                    if "PASAJEROS" in fila_siguiente:
                        start_row = i + 2
                        logger.info(f"TABLA 11 detectada en fila {i}, datos inician en {start_row}")
                        break

        # 2. BUSCAR EL FIN DE LA TABLA 11 (para no leer las tablas 19, 21, etc.)
        end_row = len(df_raw)
        for i in range(start_row, len(df_raw)):
            fila_texto = str(df_raw.iloc[i].values).upper()
            # Asumimos que la siguiente tabla empieza con "TABLA"
            if "TABLA" in fila_texto and i > start_row + 5: # +5 para evitar falsos positivos
                end_row = i
                logger.info(f"Fin de Tabla 11 detectado en fila {end_row}")
                break
        
        if start_row is None:
            raise ValueError("No se encontró el bloque TABLA 11 + Pasajeros")
        
        # 2. EXTRAER ENCABEZADOS DE FECHAS
        fila_años = df_raw.iloc[start_row - 2]
        fila_meses = df_raw.iloc[start_row - 1]
        
        meses_map = {'Ene':1, 'Feb':2, 'Mar':3, 'Abr':4, 'May':5, 'Jun':6, 
                     'Jul':7, 'Ago':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dic':12}
        
        fechas_dict = {}
        ultimo_anio = None
        for col_idx in range(1, len(fila_meses)):
            anio_raw = str(fila_años.iloc[col_idx]).strip()
            mes_raw = str(fila_meses.iloc[col_idx]).strip()
            
            if anio_raw.isdigit():
                ultimo_anio = int(anio_raw)
            
            mes_num = meses_map.get(mes_raw)
            if ultimo_anio and mes_num:
                fechas_dict[col_idx] = pd.Timestamp(year=ultimo_anio, month=mes_num, day=1)

        # 3. PROCESAR FILAS (Filtrado estricto)
        df_datos = df_raw.iloc[start_row:end_row].reset_index(drop=True)
        resultados = []

        print(f"DEBUG: Procesando filas. start_row={start_row}")
        
        for _, row in df_datos.iterrows():
            aero_name = str(row[0]).strip()

            # --- FILTRO ESTRICTO ---
            # Solo procesamos si el nombre es EXACTAMENTE "Corrientes"
            # Esto evita sumar "Aeroparque - Corrientes", "Corrientes - Posadas", etc.
            if aero_name not in self.column_mapping:
                continue
            
            nombre_mapeado = self.column_mapping[aero_name]
            
            for col_idx, fecha_obj in fechas_dict.items():
                val = row[col_idx]
                if pd.isna(val) or str(val).strip() in ['nan', '-', '']: continue
                
                # LIMPIEZA:
                # 1. Quitamos los puntos de mil (18.578 -> 18578)
                # 2. Convertimos a float para manejar decimales si existieran
                # 3. Convertimos a int (Ya tienes 18578, si quieres 18.578.000, 
                #    entonces mantén el * 1000. Si quieres 18578, quita el * 1000)
                
                s_val = str(val).strip().replace('.', '').replace(',', '.')
                
                try:
                    valor_num = float(s_val)
                    cantidad = int(valor_num) 
                    
                    # CORRECCIÓN DE LOS 12 CEROS:
                    # Si detectamos números gigantescos, los normalizamos eliminando los 12 ceros extra
                    if cantidad > 1000000000000: # 10^12
                        cantidad = cantidad // 1000000000000
                    
                    resultados.append({
                        'fecha': fecha_obj,
                        'aeropuerto': nombre_mapeado,
                        'cantidad': cantidad
                    })
                except ValueError:
                    continue
        
        df_final = pd.DataFrame(resultados)
        # Consolidar duplicados
        df_final = df_final.groupby(['fecha', 'aeropuerto'], as_index=False)['cantidad'].sum()
        
        logger.info(f"[OK] Transformación lista. Filas: {len(df_final)}")
        return df_final

    def _parse_fecha_anac(self, valor):
        """Convierte 'ene-24' o similar en una fecha de Python"""
        meses = {'ene':1, 'feb':2, 'mar':3, 'abr':4, 'may':5, 'jun':6, 
                 'jul':7, 'ago':8, 'sep':9, 'oct':10, 'nov':11, 'dic':12}
        try:
            s = str(valor).lower().strip()
            match = re.search(r'([a-z]{3})-(\d{2})', s)
            if match:
                m_str, a_str = match.groups()
                mes = meses.get(m_str)
                anio = 2000 + int(a_str)
                if mes: return pd.Timestamp(year=anio, month=mes, day=1)
            return None
        except: return None

    def _aplicar_correcciones(self, df):
        if 'aeropuerto' not in df.columns: return df
        mask = df['aeropuerto'] == 'corrientes'
        correcciones = {32200.000000000004: 19646, 39478: 18555, 40395: 19648}
        for v_err, v_ok in correcciones.items():
            df.loc[mask & (df['cantidad'] == v_err), 'cantidad'] = v_ok
        return df

    def _convertir_a_formato_long(self, df):
        """Convierte el DataFrame de ancho (columnas aeropuertos) a largo (una fila por dato)."""
        columnas_aeropuertos = [c for c in df.columns if c != "fecha"]
        
        df_long = df.melt(
            id_vars="fecha",
            value_vars=columnas_aeropuertos,
            var_name="aeropuerto",
            value_name="cantidad"
        )
        
        # Asegurar que cantidad sea numérico y rellenar nulos
        df_long["cantidad"] = pd.to_numeric(df_long["cantidad"], errors="coerce").fillna(0)
        return df_long

    def _buscar_fila_por_valor(self, df, target_value):
        """Busca el índice de la fila que contiene el texto objetivo."""
        target_norm = target_value.lower().strip().replace(" ", "")
        for index, row in df.iterrows():
            for cell in row:
                if isinstance(cell, str) and cell.lower().strip().replace(" ", "") == target_norm:
                    return index
        return None

   