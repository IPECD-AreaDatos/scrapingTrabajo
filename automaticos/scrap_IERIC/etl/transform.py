"""
TRANSFORM - Módulo de transformación de datos IERIC
Responsabilidad: Construir los 3 DataFrames del IERIC (actividad, puestos, salario)
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')

CODIGO_PROVINCIAS = {
    "Nacion": 1, "Ciudad De Buenos Aires": 2, "Buenos Aires": 6,
    "Catamarca": 10, "Cordoba": 14, "Corrientes": 18, "Chaco": 22,
    "Chubut": 26, "Entre Rios": 30, "Formosa": 34, "Jujuy": 38,
    "La Pampa": 42, "La Rioja": 46, "Mendoza": 50, "Misiones": 54,
    "Neuquen": 58, "Rio Negro": 62, "Salta": 66, "San Juan": 70,
    "San Luis": 74, "Santa Cruz": 78, "Santa Fe": 82,
    "Santiago Del Estero": 86, "Tucuman": 90, "Tierra Del Fuego": 94,
}


def _normalizar(nombre: str) -> str:
    return nombre.replace("_", " ").title()


class TransformIERIC:
    """Transforma los XLS del IERIC en 3 DataFrames."""

    def transform(self, files_dir: str = None) -> tuple:
        """
        Construye los 3 DataFrames del IERIC.

        Returns:
            tuple: (df_actividad, df_puestos, df_salario)
        """
        if files_dir is None:
            files_dir = FILES_DIR

        logger.info("[TRANSFORM] Procesando archivos IERIC desde: %s", files_dir)
        df_actividad = self._leer_actividad(files_dir)
        df_puestos   = self._leer_puestos(files_dir)
        df_salario   = self._leer_salario(files_dir)
        logger.info("[TRANSFORM] Actividad: %d | Puestos: %d | Salario: %d",
                    len(df_actividad), len(df_puestos), len(df_salario))
        return df_actividad, df_puestos, df_salario

    def _leer_actividad(self, base_dir: str) -> pd.DataFrame:
        dfs = []
        total_cargado = False
        for folder in os.listdir(base_dir):
            path = os.path.join(base_dir, folder, 'empresas_en_actividad.xls')
            if not os.path.exists(path):
                continue
            prov = _normalizar(folder)
            id_prov = CODIGO_PROVINCIAS.get(prov)
            if not id_prov:
                logger.warning("[TRANSFORM] Provincia no reconocida: %s", prov)
                continue
            try:
                df = pd.read_excel(path, skiprows=4).dropna(subset=[pd.read_excel(path, skiprows=4).columns[0]])
                df = pd.read_excel(path, skiprows=4)
                df = df.dropna(subset=[df.columns[0]])
                row = pd.DataFrame({
                    'fecha': pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date,
                    'cant_empresas': pd.to_numeric(df.iloc[:, 1], errors='coerce'),
                    'porcentaje_var_interanual': pd.to_numeric(df.iloc[:, 3], errors='coerce') / 100,
                    'id_provincia': id_prov,
                })
                dfs.append(row)
                if not total_cargado:
                    row_total = pd.DataFrame({
                        'fecha': pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date,
                        'cant_empresas': pd.to_numeric(df.iloc[:, 2], errors='coerce'),
                        'porcentaje_var_interanual': pd.to_numeric(df.iloc[:, 4], errors='coerce') / 100,
                        'id_provincia': 1,
                    })
                    dfs.append(row_total)
                    total_cargado = True
            except Exception as e:
                logger.warning("[TRANSFORM] Error leyendo actividad de %s: %s", folder, e)
        return pd.concat(dfs, ignore_index=True).dropna(subset=['fecha'])

    def _leer_puestos(self, base_dir: str) -> pd.DataFrame:
        dfs = []
        total_cargado = False
        for folder in os.listdir(base_dir):
            path = os.path.join(base_dir, folder, 'puestos_de_trabajo_registrados.xls')
            if not os.path.exists(path):
                continue
            prov = _normalizar(folder)
            id_prov = CODIGO_PROVINCIAS.get(prov)
            if not id_prov:
                continue
            try:
                df = pd.read_excel(path, skiprows=4).dropna(subset=[pd.read_excel(path, skiprows=4).columns[0]])
                df = pd.read_excel(path, skiprows=4)
                df = df.dropna(subset=[df.columns[0]])
                col_m = 5 if prov == "Buenos Aires" else 3
                col_i = 6 if prov == "Buenos Aires" else 4
                col_a = 7 if prov == "Buenos Aires" else 5
                row = pd.DataFrame({
                    'fecha': pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date,
                    'puestos_de_trabajo': pd.to_numeric(df.iloc[:, 1], errors='coerce'),
                    'porcentaje_var_mensual': (pd.to_numeric(df.iloc[:, col_m], errors='coerce') / 100).round(5),
                    'porcentaje_var_interanual': (pd.to_numeric(df.iloc[:, col_i], errors='coerce') / 100).round(5),
                    'porcentaje_var_acumulada': (pd.to_numeric(df.iloc[:, col_a], errors='coerce') / 100).round(5),
                    'id_provincia': id_prov,
                })
                dfs.append(row)
                if not total_cargado and df.shape[1] >= 11:
                    row_total = pd.DataFrame({
                        'fecha': pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date,
                        'puestos_de_trabajo': pd.to_numeric(df.iloc[:, 4], errors='coerce'),
                        'porcentaje_var_mensual': (pd.to_numeric(df.iloc[:, 8], errors='coerce') / 100).round(5),
                        'porcentaje_var_interanual': (pd.to_numeric(df.iloc[:, 9], errors='coerce') / 100).round(5),
                        'porcentaje_var_acumulada': (pd.to_numeric(df.iloc[:, 10], errors='coerce') / 100).round(5),
                        'id_provincia': 1,
                    })
                    dfs.append(row_total)
                    total_cargado = True
            except Exception as e:
                logger.warning("[TRANSFORM] Error leyendo puestos de %s: %s", folder, e)
        return pd.concat(dfs, ignore_index=True).dropna(subset=['fecha'])

    def _leer_salario(self, base_dir: str) -> pd.DataFrame:
        dfs = []
        for folder in os.listdir(base_dir):
            path = os.path.join(base_dir, folder, 'salario_promedio_construccion.xls')
            if not os.path.exists(path):
                continue
            prov = _normalizar(folder)
            id_prov = CODIGO_PROVINCIAS.get(prov)
            if not id_prov:
                continue
            try:
                df = pd.read_excel(path, skiprows=4)
                df = df.dropna(subset=[df.columns[0]])
                row = pd.DataFrame({
                    'fecha': pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date,
                    'salario_promedio': pd.to_numeric(df.iloc[:, 1], errors='coerce'),
                    'id_provincia': id_prov,
                })
                dfs.append(row)
            except Exception as e:
                logger.warning("[TRANSFORM] Error leyendo salario de %s: %s", folder, e)
        return pd.concat(dfs, ignore_index=True).dropna(subset=['fecha'])
