import pandas as pd
import numpy as np
import difflib
import re

class readDataExcel:
    def __init__(self):
        # Diccionario con nombres estandarizados
        self.provincias = {
            'BUENOS AIRES': 6,
            'CIUDAD AUTONOMA DE BUENOS AIRES': 2,
            'CATAMARCA': 10,
            'CHACO': 22,
            'CHUBUT': 26,
            'CORDOBA': 14,
            'CORRIENTES': 18,
            'ENTRE RIOS': 30,
            'FORMOSA': 34,
            'JUJUY': 38,
            'LA PAMPA': 42,
            'LA RIOJA': 46,
            'MENDOZA': 50,
            'MISIONES': 54,
            'NEUQUEN': 58,
            'RIO NEGRO': 62,
            'SALTA': 66,
            'SAN JUAN': 70,
            'SAN LUIS': 74,
            'SANTA CRUZ': 78,
            'SANTA FE': 82,
            'SANTIAGO DEL ESTERO': 86,
            'TIERRA DEL FUEGO': 94,
            'TUCUMAN': 90
        }

        self.categorias_nacion = {
            'Empleo asalariado en el sector privado': 2,
            'Empleo asalariado en el sector público': 3,
            'Empleo en casas particulares': 4,
            'Trabajo Independientes Autónomos': 5,
            'Trabajo Independientes Monotributo': 6,
            'Trabajo Independientes Monotributo Social': 7,
            'Total': 8
        }

    def _leer_excel(self, file_path, sheet_name, skiprows, drop_last_rows):
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows)
        df = df.replace({np.nan: None})
        df = df.replace(',', '.', regex=True).infer_objects(copy=False)
        if drop_last_rows > 0:
            df = df.iloc[:-drop_last_rows]
        df = df.rename(columns=lambda x: str(x).strip().replace('\n', ' '))
        return df

    def get_dataframe(self, file_path):
        lista_provincias = []
        lista_valores_estacionalidad = []
        lista_valores_sin_estacionalidad = []
        lista_registro = []
        lista_fechas = []

        self.listado_provincias(file_path, lista_provincias, lista_valores_estacionalidad,
                                lista_valores_sin_estacionalidad, lista_registro, lista_fechas)
        self.listado_nacion(file_path, lista_provincias, lista_valores_estacionalidad,
                            lista_valores_sin_estacionalidad, lista_registro, lista_fechas)

        df = pd.DataFrame({
            'fecha': lista_fechas,
            'id_provincia': lista_provincias,
            'id_tipo_registro': lista_registro,
            'cantidad_con_estacionalidad': lista_valores_estacionalidad,
            'cantidad_sin_estacionalidad': lista_valores_sin_estacionalidad
        })

        return df.sort_values(by=['fecha', 'id_provincia', 'id_tipo_registro'])

    def normalizar_texto(self, texto):
        if not isinstance(texto, str):
            return ''
        return (
            texto.upper()
            .replace('Á', 'A').replace('É', 'E').replace('Í', 'I')
            .replace('Ó', 'O').replace('Ú', 'U')
            .replace('\n', ' ').replace('  ', ' ').strip()
        )

    def limpiar_nombre(self, nombre):
        nombre = self.normalizar_texto(nombre)
        nombre = re.sub(r'\d+/?', '', nombre)  # elimina números y barras
        nombre = re.sub(r'[^A-ZÑ ]', '', nombre)  # solo letras
        nombre = nombre.replace("CDAD AUTONOMA", "CIUDAD AUTONOMA")  # alias común
        nombre = nombre.replace("CIUDAD DE BUENOS AIRES", "CIUDAD AUTONOMA DE BUENOS AIRES")
        nombre = re.sub(r'\s+', ' ', nombre)  # múltiples espacios por uno
        return nombre.strip()


    def listado_provincias(self, file_path, lista_provincias, lista_valores_estacionalidad,
                        lista_valores_sin_estacionalidad, lista_registro, lista_fechas):
        try:
            df_estacional = self._leer_excel(file_path, sheet_name=13, skiprows=1, drop_last_rows=5)
            df_no_estacional = self._leer_excel(file_path, sheet_name=14, skiprows=1, drop_last_rows=6)

            df_estacional['Período'] = pd.date_range(start='2009-01-01', periods=len(df_estacional), freq='MS')
            df_no_estacional['Período'] = pd.date_range(start='2009-01-01', periods=len(df_no_estacional), freq='MS')

            columnas_estandar = [self.limpiar_nombre(c) for c in df_estacional.columns]

            for (_, row_e), (_, row_ne) in zip(df_estacional.iterrows(), df_no_estacional.iterrows()):
                fecha = row_e['Período']

                for col_original, col_limpia in zip(df_estacional.columns, columnas_estandar):
                    # Evitamos columnas que no son provincias
                    if col_limpia in ("PERIODO", "") or col_limpia.startswith("UNNAMED"):
                        continue

                    if col_limpia in self.provincias:
                        id_provincia = self.provincias[col_limpia]
                        lista_valores_estacionalidad.append(row_e.get(col_original))
                        lista_valores_sin_estacionalidad.append(row_ne.get(col_original))
                        lista_provincias.append(id_provincia)
                        lista_registro.append(1)
                        lista_fechas.append(fecha)
                    else:
                        print(f"⚠️ No se reconoció la provincia: {col_limpia}")

        except Exception as e:
            print(f"❌ Error cargando provincias: {e}")

    def listado_nacion(self, file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro, lista_fechas):
        try:
            df_estacional = self._leer_excel(file_path, sheet_name=3, skiprows=1, drop_last_rows=6)
            df_no_estacional = self._leer_excel(file_path, sheet_name=4, skiprows=1, drop_last_rows=8)

            df_estacional['Período'] = pd.date_range(start='2012-01-01', periods=len(df_estacional), freq='MS')
            df_no_estacional['Período'] = pd.date_range(start='2012-01-01', periods=len(df_no_estacional), freq='MS')

            for (_, row_e), (_, row_ne) in zip(df_estacional.iterrows(), df_no_estacional.iterrows()):
                fecha = row_e['Período']
                for categoria, id_registro in self.categorias_nacion.items():
                    lista_valores_estacionalidad.append(row_e.get(categoria))
                    lista_valores_sin_estacionalidad.append(row_ne.get(categoria))
                    lista_provincias.append(1)
                    lista_registro.append(id_registro)
                    lista_fechas.append(fecha)

        except Exception as e:
            print(f"❌ Error cargando Nación: {e}")
