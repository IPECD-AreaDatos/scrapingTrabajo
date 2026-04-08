"""
TRANSFORM - Módulo de transformación de datos Supermercados
Responsabilidad: Construir el DataFrame consolidado por provincia desde el XLS
"""
import os
import logging
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'encuesta_supermercado.xls'

LISTA_PROVINCIAS = [
    ['Total', 1, 1],
    ['Ciudad Autónoma de Buenos Aires', 2, 2],
    ['24 partidos del Gran Buenos Aires ', 6, 2],
    ['Resto de Buenos Aires', 6, 3],
    ['Catamarca', 10, 4], ['Chaco', 22, 5], ['Chubut', 26, 7],
    ['Córdoba', 14, 3], ['Corrientes', 18, 5], ['Entre Ríos', 30, 3],
    ['Formosa', 34, 5], ['Jujuy', 38, 4], ['La Pampa', 42, 3],
    ['La Rioja', 46, 4], ['Mendoza', 50, 6], ['Misiones', 54, 5],
    ['Neuquén', 58, 7], ['Río Negro', 62, 7], ['Salta', 66, 4],
    ['San Juan', 70, 6], ['San Luis', 74, 6], ['Santa Cruz', 78, 7],
    ['Santa Fe', 82, 3], ['Santiago del Estero', 86, 4],
    ['Tierra del Fuego', 94, 7], ['Tucumán', 90, 4],
]

NOMBRES_COLUMNAS = [
    'id_provincia', 'fecha', 'total_facturacion', 'bebidas', 'almacen',
    'panaderia', 'lacteos', 'carnes', 'verduleria_fruteria',
    'alimentos_preparados_rostiseria', 'articulos_limpieza_perfumeria',
    'indumentaria_calzado_textiles_hogar', 'electronica_hogar', 'otros'
]

COLUMNAS_ESTIMATIVAS = [
    'alimentos_preparados_rostiseria', 'indumentaria_calzado_textiles_hogar', 'electronica_hogar'
]


class TransformSupermercados:
    """Transforma el XLS de supermercados en un DataFrame consolidado por provincia."""

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        """
        Construye el DataFrame final con datos de todas las provincias.

        Returns:
            pd.DataFrame consolidado
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Leyendo archivo: %s", ruta_archivo)
        
        # 1. Calculamos tamaño mirando la columna B (Ventas Totales) que es más estable
        # Usamos usecols=[1] (Columna B) y saltamos las primeras filas de títulos
        df_aux = pd.read_excel(ruta_archivo, sheet_name=5, skiprows=6, usecols=[1], header=None)
        
        # Nueva lógica de tamaño: contamos cuántos valores numéricos reales hay
        # Esto ignora el "ene-26*" con asterisco y las notas al pie automáticamente
        tamaño = len(pd.to_numeric(df_aux.iloc[:, 0].astype(str).str.replace('.', '', regex=False), errors='coerce').dropna())
        
        logger.info(f"[TRANSFORM] Meses detectados: {tamaño}")

        # 2. Cargamos el DF completo para buscar horizontalmente en la fila 3
        df = pd.read_excel(ruta_archivo, sheet_name=5, header=None)

        fecha_inicio = date(2017, 1, 1)
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(tamaño)]

        dfs = []
        for provincia in LISTA_PROVINCIAS:
            try:
                nombre_buscado = provincia[0].strip().lower()
                if nombre_buscado == 'total': nombre_buscado = 'total del país'
                
                col_idx = -1
                for i, valor_celda in enumerate(df.iloc[2, :]):
                    valor_limpio = str(valor_celda).strip().lower()
                    if nombre_buscado in valor_limpio:
                        col_idx = i
                        break
                
                if col_idx == -1:
                    continue

                # 3. Extraemos el bloque de datos: Fila 7 (índice 6) + cantidad de meses
                df_prov = df.iloc[6 : 6 + tamaño, col_idx : col_idx + 12].copy()
                
                df_prov.columns = NOMBRES_COLUMNAS[2:]
                df_prov.insert(0, 'id_provincia', provincia[1])
                df_prov.insert(1, 'fecha', lista_fechas)
                df_prov['id_region'] = int(provincia[2])
                
                dfs.append(df_prov)

            except Exception as e:
                logger.error(f"[TRANSFORM] Error en {provincia[0]}: {e}")

        df_final = pd.concat(dfs, ignore_index=True)

        # Limpieza final de números
        for col in NOMBRES_COLUMNAS[2:]:
            # 1. Convertimos a string y quitamos espacios
            df_final[col] = df_final[col].astype(str).str.strip()
            
            # 2. Reemplazamos la 's' (secreto estadístico) por NaN
            df_final[col] = df_final[col].replace(['s', 'S', 'nan', 'None'], pd.NA)
            
            # 3. Convertimos a numérico 
            # Si el Excel tiene puntos como miles, pandas suele leerlo bien.
            # Si el punto está molestando, lo tratamos como flotante.
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            # 4. Redondeamos a 5 decimales
            df_final[col] = df_final[col].round(5)

        logger.info("[TRANSFORM] DataFrame final: %d filas", len(df_final))
        return df_final