import os
import pandas as pd

codigo_provincias = {
    "Nacion": 1,
    "Ciudad De Buenos Aires": 2,
    "Buenos Aires": 6,
    "Catamarca": 10,
    "Cordoba": 14,
    "Corrientes": 18,
    "Chaco": 22,
    "Chubut": 26,
    "Entre Rios": 30,
    "Formosa": 34,
    "Jujuy": 38,
    "La Pampa": 42,
    "La Rioja": 46,
    "Mendoza": 50,
    "Misiones": 54,
    "Neuquen": 58,
    "Rio Negro": 62,
    "Salta": 66,
    "San Juan": 70,
    "San Luis": 74,
    "Santa Cruz": 78,
    "Santa Fe": 82,
    "Santiago Del Estero": 86,
    "Tucuman": 90,
    "Tierra Del Fuego": 94
}

def normalize_province_name(folder_name):
    return folder_name.replace("_", " ").title()

class readFileActividad:
    def __init__(self):
        self.total_pais_cargado = False

    def read_file(self):
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        dfs = []

        for provincia_folder in os.listdir(base_dir):
            path_file = os.path.join(base_dir, provincia_folder, 'empresas_en_actividad.xls')
            if not os.path.exists(path_file):
                continue

            provincia_normalizada = normalize_province_name(provincia_folder)
            id_provincia = codigo_provincias.get(provincia_normalizada)

            if not id_provincia:
                print(f"⚠️ Provincia no reconocida: {provincia_normalizada}")
                continue

            try:
                df = pd.read_excel(path_file, skiprows=4)
                df = df.dropna(subset=[df.columns[0]])

                # 1. Provincia actual
                df_prov = pd.DataFrame()
                df_prov['fecha'] = pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date
                df_prov['cant_empresas'] = pd.to_numeric(df.iloc[:, 1], errors='coerce')
                df_prov['porcentaje_var_interanual'] = pd.to_numeric(df.iloc[:, 3], errors='coerce')
                df_prov['id_provincia'] = id_provincia
                dfs.append(df_prov)

                # 2. Total País
                if not self.total_pais_cargado:
                    df_total = pd.DataFrame()
                    df_total['fecha'] = pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date
                    df_total['cant_empresas'] = pd.to_numeric(df.iloc[:, 2], errors='coerce')
                    df_total['porcentaje_var_interanual'] = pd.to_numeric(df.iloc[:, 4], errors='coerce')
                    df_total['id_provincia'] = 1
                    dfs.append(df_total)
                    self.total_pais_cargado = True

            except Exception as e:
                print(f"❌ Error leyendo {path_file}: {e}")

        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final[['fecha', 'id_provincia', 'cant_empresas', 'porcentaje_var_interanual']]
        df_final = df_final.dropna(subset=['fecha'])

        # 🧠 Corrección porcentajes
        df_final['porcentaje_var_interanual'] = df_final['porcentaje_var_interanual'].fillna(0) / 100

        return df_final
