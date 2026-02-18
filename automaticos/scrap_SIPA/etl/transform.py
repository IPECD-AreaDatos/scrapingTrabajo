"""
TRANSFORM - Módulo de transformación de datos SIPA
Responsabilidad: Leer el Excel y construir el DataFrame de SIPA
"""
import logging
import pandas as pd
import numpy as np
import re

logger = logging.getLogger(__name__)

PROVINCIAS = {
    'BUENOS AIRES': 6, 'CIUDAD AUTONOMA DE BUENOS AIRES': 2, 'CATAMARCA': 10,
    'CHACO': 22, 'CHUBUT': 26, 'CORDOBA': 14, 'CORRIENTES': 18,
    'ENTRE RIOS': 30, 'FORMOSA': 34, 'JUJUY': 38, 'LA PAMPA': 42,
    'LA RIOJA': 46, 'MENDOZA': 50, 'MISIONES': 54, 'NEUQUEN': 58,
    'RIO NEGRO': 62, 'SALTA': 66, 'SAN JUAN': 70, 'SAN LUIS': 74,
    'SANTA CRUZ': 78, 'SANTA FE': 82, 'SANTIAGO DEL ESTERO': 86,
    'TIERRA DEL FUEGO': 94, 'TUCUMAN': 90
}
CATEGORIAS_NACION = {
    'Empleo asalariado en el sector privado': 2,
    'Empleo asalariado en el sector público': 3,
    'Empleo en casas particulares': 4,
    'Trabajo Independientes Autónomos': 5,
    'Trabajo Independientes Monotributo': 6,
    'Trabajo Independientes Monotributo Social': 7,
    'Total': 8
}


class TransformSIPA:
    """Transforma el Excel de SIPA en un DataFrame normalizado."""

    def transform(self, ruta: str) -> pd.DataFrame:
        """
        Lee el Excel y construye el DataFrame de SIPA.

        Returns:
            pd.DataFrame con columnas: fecha, id_provincia, id_tipo_registro,
                                       cantidad_con_estacionalidad, cantidad_sin_estacionalidad
        """
        logger.info("[TRANSFORM] Leyendo Excel: %s", ruta)
        listas = {'provincias': [], 'est': [], 'sin_est': [], 'registro': [], 'fechas': []}

        self._listado_provincias(ruta, listas)
        self._listado_nacion(ruta, listas)

        df = pd.DataFrame({
            'fecha': listas['fechas'],
            'id_provincia': listas['provincias'],
            'id_tipo_registro': listas['registro'],
            'cantidad_con_estacionalidad': listas['est'],
            'cantidad_sin_estacionalidad': listas['sin_est'],
        })
        df = df.sort_values(by=['fecha', 'id_provincia', 'id_tipo_registro'])
        logger.info("[TRANSFORM] SIPA: %d filas", len(df))
        return df

    def _leer_excel(self, ruta, sheet_name, skiprows):
        df = pd.read_excel(ruta, sheet_name=sheet_name, skiprows=skiprows)
        df = df.replace({np.nan: None}).replace(',', '.', regex=True).infer_objects()
        df = df[~df.iloc[:, 0].astype(str).str.contains("nota", case=False, na=False)]
        df = df[df.iloc[:, 1:].notnull().any(axis=1)]
        df = df.rename(columns=lambda x: str(x).strip().replace('\n', ' '))
        return df

    def _normalizar(self, texto):
        if not isinstance(texto, str):
            return ''
        return (texto.upper()
                .replace('Á','A').replace('É','E').replace('Í','I')
                .replace('Ó','O').replace('Ú','U')
                .replace('\n',' ').replace('  ',' ').strip())

    def _limpiar_nombre(self, nombre):
        nombre = self._normalizar(nombre)
        nombre = re.sub(r'\d+/?', '', nombre)
        nombre = re.sub(r'[^A-ZÑ ]', '', nombre)
        nombre = nombre.replace("CDAD AUTONOMA", "CIUDAD AUTONOMA")
        nombre = nombre.replace("CIUDAD DE BUENOS AIRES", "CIUDAD AUTONOMA DE BUENOS AIRES")
        return re.sub(r'\s+', ' ', nombre).strip()

    def _listado_provincias(self, ruta, listas):
        try:
            df_e  = self._leer_excel(ruta, 13, 1)
            df_ne = self._leer_excel(ruta, 14, 1)
            df_e['Período']  = pd.date_range(start='2009-01-01', periods=len(df_e),  freq='MS')
            df_ne['Período'] = pd.date_range(start='2009-01-01', periods=len(df_ne), freq='MS')
            cols_std = [self._limpiar_nombre(c) for c in df_e.columns]
            for (_, row_e), (_, row_ne) in zip(df_e.iterrows(), df_ne.iterrows()):
                fecha = row_e['Período']
                for col_orig, col_std in zip(df_e.columns, cols_std):
                    if col_std in ("PERIODO", "") or col_std.startswith("UNNAMED"):
                        continue
                    if col_std in PROVINCIAS:
                        listas['provincias'].append(PROVINCIAS[col_std])
                        listas['est'].append(row_e.get(col_orig))
                        listas['sin_est'].append(row_ne.get(col_orig))
                        listas['registro'].append(1)
                        listas['fechas'].append(fecha)
        except Exception as e:
            logger.error("[TRANSFORM] Error cargando provincias: %s", e)

    def _listado_nacion(self, ruta, listas):
        try:
            df_e  = self._leer_excel(ruta, 3, 1)
            df_ne = self._leer_excel(ruta, 4, 1)
            df_e['Período']  = pd.date_range(start='2012-01-01', periods=len(df_e),  freq='MS')
            df_ne['Período'] = pd.date_range(start='2012-01-01', periods=len(df_ne), freq='MS')
            for (_, row_e), (_, row_ne) in zip(df_e.iterrows(), df_ne.iterrows()):
                fecha = row_e['Período']
                for cat, id_reg in CATEGORIAS_NACION.items():
                    listas['provincias'].append(1)
                    listas['est'].append(row_e.get(cat))
                    listas['sin_est'].append(row_ne.get(cat))
                    listas['registro'].append(id_reg)
                    listas['fechas'].append(fecha)
        except Exception as e:
            logger.error("[TRANSFORM] Error cargando Nación: %s", e)
