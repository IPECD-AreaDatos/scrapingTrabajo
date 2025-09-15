from src.shared.helpers import limpiar_nombree, contar_meses_validos
from src.shared.mappings import provincias_superm
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from datetime import date
from decimal import Decimal

def transform_supermercado_data(file_path):
    """
    Transforma el Excel de encuesta de supermercados en un DataFrame limpio
    con provincias, fechas y métricas.
    """

    # === Paso 1: determinar cantidad de meses por sección
    df_aux = pd.read_excel(
        file_path,
        sheet_name=5,
        skiprows=5,
        usecols='c',
        names=['fecha']
    )
    meses_por_seccion = contar_meses_validos(df_aux['fecha'])

    # === Paso 2: leer el archivo principal
    columnas = [
        'id_provincia_indec', 'fecha', 'total_facturacion', 'bebidas',
        'almacen', 'panaderia', 'lacteos', 'carnes', 'verduleria_fruteria',
        'alimentos_preparados_rostiseria',
        'articulos_limpieza_perfumeria',
        'indumentaria_calzado_textiles_hogar',
        'electronica_hogar', 'otros'
    ]

    df = pd.read_excel(
        file_path,
        sheet_name=5,
        skiprows=2,
        usecols='a,c,d,e,f,g,h,i,j,k,l,m,n,o',
        names=columnas
    )

    # === Paso 3: generar fechas
    fecha_inicio = date(2017, 1, 1)
    lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(meses_por_seccion)]

    # === Paso 4: recorrer provincias y construir dataset final
    df_final = pd.DataFrame(columns=columnas + ['id_region_indec'])

    for nombre, id_prov, id_region in provincias_superm:
        fila, _ = df[df == nombre].stack().index[0]
        fila += 1  # compensar desfase
        df_prov = df.iloc[fila: fila + meses_por_seccion].copy()
        df_prov['id_provincia_indec'] = id_prov
        df_prov['id_region_indec'] = id_region
        df_prov['fecha'] = lista_fechas
        df_final = pd.concat([df_final, df_prov])

    # === Paso 5: limpieza final
    columnas_numericas = [
        'total_facturacion', 'bebidas', 'almacen', 'panaderia', 'lacteos',
        'carnes', 'verduleria_fruteria', 'alimentos_preparados_rostiseria',
        'articulos_limpieza_perfumeria',
        'indumentaria_calzado_textiles_hogar', 'electronica_hogar', 'otros'
    ]

    # Convertir strings a Decimal preservando exactitud
    for col in columnas_numericas:
        df_final[col] = (
            df_final[col]
            .replace('s', np.nan)  # limpiar valores erróneos
            .apply(lambda x: Decimal(str(x).replace(',', '.')) if pd.notnull(x) else None)
        )

    df_final['id_provincia_indec'] = df_final['id_provincia_indec'].astype(int)
    df_final['id_region_indec'] = df_final['id_region_indec'].astype(int)

    # === Paso 6: reordenar columnas
    columnas_ordenadas = [
        'fecha', 'id_region_indec', 'id_provincia_indec'
    ] + [col for col in df_final.columns if col not in ['fecha', 'id_region_indec', 'id_provincia_indec']]

    df_final = df_final[columnas_ordenadas]

    return df_final.reset_index(drop=True)