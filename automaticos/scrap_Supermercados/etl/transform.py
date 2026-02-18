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
    'id_provincia_indec', 'fecha', 'total_facturacion', 'bebidas', 'almacen',
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
        df_aux = pd.read_excel(ruta_archivo, sheet_name=5, skiprows=5, usecols='c', names=['fecha'])
        tamaño = self._calcular_tamaño(df_aux['fecha'])

        df = pd.read_excel(ruta_archivo, sheet_name=5, skiprows=2,
                           usecols='a,c,d,e,f,g,h,i,j,k,l,m,n,o', names=NOMBRES_COLUMNAS)

        fecha_inicio = date(2017, 1, 1)
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(tamaño)]

        dfs = []
        for provincia in LISTA_PROVINCIAS:
            try:
                fila, _ = df[df == provincia[0]].stack().index[0]
                fila += 1
                df_prov = df.iloc[fila: fila + tamaño].copy()
                df_prov['id_provincia_indec'] = provincia[1]
                df_prov['id_region_indec'] = int(provincia[2])
                df_prov['fecha'] = lista_fechas
                dfs.append(df_prov)
            except IndexError:
                logger.warning("[TRANSFORM] Provincia no encontrada: %s", provincia[0])

        df_final = pd.concat(dfs, ignore_index=True)
        df_final[COLUMNAS_ESTIMATIVAS] = df_final[COLUMNAS_ESTIMATIVAS].replace('s', None)
        df_final[COLUMNAS_ESTIMATIVAS] = df_final[COLUMNAS_ESTIMATIVAS].apply(
            lambda col: col.apply(lambda x: float(x) if pd.notnull(x) else None)
        )
        logger.info("[TRANSFORM] DataFrame final: %d filas", len(df_final))
        return df_final

    @staticmethod
    def _calcular_tamaño(serie: pd.Series) -> int:
        count = 0
        for val in serie:
            if str(val) == 'nan':
                break
            count += 1
        return count
