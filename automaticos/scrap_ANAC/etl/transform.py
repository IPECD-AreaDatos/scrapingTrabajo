"""
TRANSFORM - Módulo de transformación de datos ANAC
Responsabilidad: Procesar y transformar datos del Excel
"""
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

logger = logging.getLogger(__name__)

class TransformANAC:
    """Clase para transformar datos de ANAC"""
    
    def __init__(self, target_value="TABLA 11"):
        """
        Inicializa el transformador
        
        Args:
            target_value: Valor a buscar en el archivo (por defecto "TABLA 11")
        """
        self.target_value = target_value
        
        # Diccionario de mapeo de nombres de columnas
        self.column_mapping = {
            'fecha': 'fecha',
            'Aeroparque': 'aeroparque',
            'Bahía Blanca': 'bahia_blanca',
            'Bariloche': 'bariloche',
            'Base Marambio': 'base_marambio',
            'Catamarca': 'catamarca',
            'Chapelco': 'chapelco',
            'Comod. Rivadavia': 'comod_rivadavia',
            'Concordia': 'concordia',
            'Córdoba': 'cordoba',
            'Corrientes': 'corrientes',
            'El Calafate': 'el_calafate',
            'El Palomar': 'el_palomar',
            'Esquel': 'esquel',
            'Ezeiza': 'ezeiza',
            'Formosa': 'formosa',
            'General Pico': 'general_pico',
            'Goya': 'goya',
            'Gualeguaychú': 'gualeguaychu',
            'Iguazú': 'iguazu',
            'Jujuy': 'jujuy',
            'Junín': 'junin',
            'La Plata': 'la_plata',
            'La Rioja': 'la_rioja',
            'Malargüe': 'malargue',
            'Mar del Plata': 'mar_del_plata',
            'Mendoza': 'mendoza',
            'Moreno': 'moreno',
            'Morón': 'moron',
            'Neuquén': 'neuquen',
            'Paraná': 'parana',
            'Paso de los Libres': 'paso_de_los_libres',
            'Posadas': 'posadas',
            'Puerto Madryn': 'puerto_madryn',
            'Reconquista': 'reconquista',
            'Resistencia': 'resistencia',
            'Río Cuarto': 'rio_cuarto',
            'Río Gallegos': 'rio_gallegos',
            'Río Grande': 'rio_grande',
            'Rosario': 'rosario',
            'Salta': 'salta',
            'San Fernando': 'san_fernando',
            'San Juan': 'san_juan',
            'San Luis': 'san_luis',
            'San Rafael': 'san_rafael',
            'Santa Fe': 'santa_fe',
            'Santa Rosa': 'santa_rosa',
            'Santa Rosa de Conlara': 'santa_rosa_de_conlara',
            'Santiago del Estero': 'santiago_del_estero',
            'Tandil': 'tandil',
            'Termas Río Hondo': 'termas_rio_hondo',
            'Trelew': 'trelew',
            'Tucumán': 'tucuman',
            'Ushuaia': 'ushuaia',
            'Viedma': 'viedma',
            'Villa Gesell': 'villa_gesell',
            'Villa Reynolds': 'villa_reynolds',
            'Otros': 'otros'
        }
        
        # Columnas a convertir a numérico y multiplicar por 1000
        self.columnas_numericas = list(self.column_mapping.values())
        self.columnas_numericas.remove('fecha')

    def transform(self, file_path):
        """
        Transforma el archivo Excel en un DataFrame procesado
        """
        try:
            logger.info(f"Leyendo archivo Excel: {file_path}")
            df = pd.read_excel(file_path, sheet_name=0)
            
            # Buscar la fila objetivo (donde aparece "TABLA 11")
            fila_target = self._buscar_fila_por_valor(df, self.target_value)
            if fila_target is None:
                raise ValueError(f"No se encontró el valor '{self.target_value}' en el archivo.")
            
            # === 1) Encontrar inicio real de TABLA 11 ===
            fila_inicio = fila_target + 2   # La tabla empieza 2 filas después
            
            # === 2) Encontrar final real de la tabla ===
            fila_fin = fila_inicio
            while fila_fin < len(df) and not df.iloc[fila_fin].isna().all():
                fila_fin += 1

            logger.info(f"Filas detectadas para TABLA 11: inicio={fila_inicio}, fin={fila_fin}")

            # === 3) Extraer fechas ANTES de recortar ===
            self.fechas = df.iloc[fila_inicio:fila_fin].iloc[:, 0].tolist()

            # === 4) Recortamos SOLO la tabla ===
            df = df.iloc[fila_inicio:fila_fin].copy()

            logger.info(f"Filtrado final TABLA 11: {len(df)} filas")

            # Eliminar filas que no tienen al menos 2 valores numéricos
            df = df[df.apply(lambda row: pd.to_numeric(row, errors="coerce").count() >= 2, axis=1)]

            # Limpiar columnas vacías
            df.dropna(axis=1, how='all', inplace=True)
            
            # Transponer y renombrar columnas
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df.drop(df.index[0]).reset_index(drop=True)

            print("\n=== TABLA TRANSFORMADA ANTES DE FECHAS ===")

            # Fecha inicial del histórico ANAC
            fecha_inicio = pd.Timestamp("2023-01-01")

            # Generar una fecha por cada fila
            fechas_reales = [
                fecha_inicio + pd.DateOffset(months=i)
                for i in range(len(df))
            ]

            # Insertar fechas generadas
            df.insert(0, "fecha", fechas_reales)

            logger.info(f"[OK] Fechas generadas automáticamente ({len(fechas_reales)})")

            print("\n=== TABLA CON FECHAS CORRECTAS ===")

            # Eliminar última columna None si existe
            if df.columns[-1] is None:
                df.drop(df.columns[-1], axis=1, inplace=True)
            
            # Convertir columnas a numerico y multiplicar por 1000
            self._convertir_columnas_numericas(df)
            
            # Renombrar columnas
            df.rename(columns=self.column_mapping, inplace=True)
            
            # Aplicar correcciones específicas
            df = self._aplicar_correcciones(df)

            # Convertir al formato long
            df = self._convertir_a_formato_long(df)

            print(df)

            print(df.columns.tolist())
        
            logger.info(f"[OK] DataFrame transformado: {len(df)} filas, {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error en transformación: {e}")
            raise

    def debug_tablas(self, df):
        for i, row in df.iterrows():
            for j, cell in enumerate(row):
                if isinstance(cell, str) and "tabla" in cell.lower():
                    print(f"Fila {i}, Col {j}: '{cell}'")

    def _convertir_a_formato_long(self, df):
        # Columnas que no son aeropuertos
        columnas_id = ["fecha"]

        # Todas las demás columnas son aeropuertos
        columnas_aeropuertos = [c for c in df.columns if c not in columnas_id]

        # Convertimos usando melt()
        df_long = df.melt(
            id_vars=columnas_id,
            value_vars=columnas_aeropuertos,
            var_name="aeropuerto",
            value_name="cantidad"
        )

        # Asegurar valores numéricos
        df_long["cantidad"] = pd.to_numeric(df_long["cantidad"], errors="coerce").fillna(0)

        return df_long

    def _buscar_fila_por_valor(self, df, target_value):
        target_norm = target_value.lower().strip()

        for index, row in df.iterrows():
            for cell in row:
                if not isinstance(cell, str):
                    continue
                
                cell_norm = cell.lower().strip()

                # match estricto de palabra entera
                if cell_norm == target_norm:
                    return index

                # match flexible seguro
                if cell_norm.startswith(target_norm + " "):
                    return index

                if cell_norm.startswith(target_norm + "\n"):
                    return index

                # TABLA 11 separado por espacios
                if cell_norm.replace(" ", "") == target_norm.replace(" ", ""):
                    return index

        return None

    def _convertir_columnas_numericas(self, df):
        """Convierte columnas a numérico y multiplica por 1000"""
        columnas_originales = [k for k, v in self.column_mapping.items() if k != 'fecha']
        
        for col in columnas_originales:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce') * 1000

    def _parsear_fechas_desde_excel(self, indices_fechas):
        """
        Parsea fechas desde el formato del Excel (ej: "dic-18", "ene-19", "feb-25")
        
        Args:
            indices_fechas: Lista de strings con fechas en formato "mes-año"
            
        Returns:
            list: Lista de objetos date
        """
        meses_dict = {
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
        }
        
        fechas_parseadas = []
        
        for fecha_str in indices_fechas:
            try:
                # Limpiar el string
                fecha_str = str(fecha_str).strip().lower()
                
                # Buscar patrón "mes-año" o "mes-año" con espacios
                partes = fecha_str.split('-')
                if len(partes) >= 2:
                    mes_str = partes[0].strip()
                    año_str = partes[1].strip()
                    
                    # Obtener mes
                    mes = meses_dict.get(mes_str)
                    if mes is None:
                        # Intentar con nombres completos
                        for key, value in meses_dict.items():
                            if key in mes_str:
                                mes = value
                                break
                    
                    if mes is None:
                        raise ValueError(f"Mes no reconocido: {mes_str}")
                    
                    # Obtener año (puede ser 18, 19, 20, 21, 22, 23, 24, 25)
                    año = int(año_str)
                    # Convertir años de 2 dígitos a 4 dígitos
                    if año < 50:  # Asumimos que 18-49 son 2018-2049
                        año = 2000 + año
                    elif año < 100:  # 50-99 serían 1950-1999 (poco probable)
                        año = 1900 + año
                    
                    # Crear fecha (primer día del mes)
                    fecha = datetime(año, mes, 1).date()
                    fechas_parseadas.append(fecha)
                else:
                    # Si no se puede parsear, intentar con datetime
                    try:
                        fecha = pd.to_datetime(fecha_str).date()
                        fechas_parseadas.append(fecha)
                    except:
                        logger.warning(f"No se pudo parsear fecha: {fecha_str}")
                        fechas_parseadas.append(None)
                        
            except Exception as e:
                logger.warning(f"Error parseando fecha '{fecha_str}': {e}")
                fechas_parseadas.append(None)
        
        return fechas_parseadas
    
    def _corregir_fechas_faltantes(self, fechas):
        """
        Corrige fechas None inferiéndolas desde la primera fecha válida
        
        Args:
            fechas: Lista de fechas (puede contener None)
            
        Returns:
            list: Lista de fechas corregidas
        """
        if not any(f is None for f in fechas):
            return fechas
        
        logger.warning("Algunas fechas no se pudieron parsear. Intentando inferir...")
        
        # Buscar primera fecha válida
        primera_fecha_valida = None
        primera_posicion = None
        for i, f in enumerate(fechas):
            if f is not None:
                primera_fecha_valida = f
                primera_posicion = i
                break
        
        if primera_fecha_valida:
            # Generar fechas desde la primera válida
            fechas_corregidas = []
            for i in range(len(fechas)):
                if fechas[i] is not None:
                    fechas_corregidas.append(fechas[i])
                else:
                    # Inferir desde la primera fecha válida
                    meses_desde_inicio = i - primera_posicion
                    fecha_inferida = primera_fecha_valida + relativedelta(months=meses_desde_inicio)
                    fechas_corregidas.append(fecha_inferida)
            return fechas_corregidas
        
        return fechas

    def _aplicar_correcciones(self, df):
        """
        Aplica correcciones específicas a los datos de Corrientes
        
        Args:
            df: DataFrame a corregir
            
        Returns:
            pd.DataFrame: DataFrame corregido
        """
        logger.info("Aplicando correcciones específicas a datos de Corrientes...")
        
        # Correcciones específicas identificadas
        correcciones = {
            32200.000000000004: 19646,
            39478: 18555,
            40395: 19648
        }
        
        for valor_incorrecto, valor_correcto in correcciones.items():
            if 'corrientes' in df.columns:
                cantidad = (df["corrientes"] == valor_incorrecto).sum()
                if cantidad > 0:
                    df.loc[df["corrientes"] == valor_incorrecto, "corrientes"] = valor_correcto
                    logger.info(f"[OK] Corregidos {cantidad} registros: {valor_incorrecto} -> {valor_correcto}")
        
        return df