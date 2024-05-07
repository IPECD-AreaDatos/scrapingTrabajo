from matplotlib.dates import relativedelta
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

class armadoDF:
    def armadoDF(file_path):
        # Leer el archivo xlsx y cargarlo en un DataFrame
        df = pd.read_excel(file_path, index_col=None) 

        target_value = "TABLA 11"

        fila_target = ArmadoDF.buscar_fila_por_valor(df, target_value) + 2

        print("Número de fila:", fila_target)
    
        df = df.iloc[fila_target:(fila_target + 58)]
        # Borrar columnas que tienen solo None
        df = df.loc[:, df.isnull().sum() < len(df)]
        df = df.transpose()
        # Renombrar las columnas con el nombre de la primera fila
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        
        # Crear una nueva columna con las fechas
        fechas = []
        fecha_inicio = datetime.strptime("2019-01-01", "%Y-%m-%d").date()  # Obtenemos solo la parte de la fecha
        for i in range(len(df)):
            fechas.append(fecha_inicio + relativedelta(months=i))

        df.insert(0, 'fecha', fechas)
        df = df.drop(df.columns[-1], axis=1)
        df.reset_index(drop=True, inplace=True)
        print(df)
        print(df.columns)
        print(df.dtypes)
        return df
        

        

class ArmadoDF:
    @staticmethod
    def buscar_fila_por_valor(df, target_value):
        # Buscar el valor en todas las columnas del DataFrame
        for index, row in df.iterrows():
            if target_value in row.values:
                return index  # Devolver el índice de la fila si se encuentra el valor
        return None  # Devolver None si el valor no se encuentra en ninguna fila
