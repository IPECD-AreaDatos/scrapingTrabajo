from matplotlib.dates import relativedelta
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

class armadoDF:
    def armadoDF(file_path):
        # Leer el archivo xlsx y cargarlo en un DataFrame
        sheet_name ='OUT'
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        target_value = "TABLA 11"

        fila_target = ArmadoDF.buscar_fila_por_valor(df, target_value) + 2

        print("Número de fila:", fila_target)
    
        df = df.iloc[fila_target:(fila_target + 58)]
        # Borrar columnas que tienen solo None
        print("Columnas antes de eliminar:", df.columns)
        df = df.dropna(axis=1, how='all')
        print("Columnas después de eliminar:", df.columns)
        print(df)
        exit(0)
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
        
        colums = ['Aeroparque', 'Bahía Blanca', 'Bariloche', 'Base Marambio',    
       'Catamarca', 'Chapelco', 'Comod. Rivadavia', 'Concordia', 'Córdoba',    
       'Corrientes', 'El Calafate', 'El Palomar', 'Esquel', 'Ezeiza',
       'Formosa', 'General Pico', 'Goya', 'Gualeguaychú', 'Iguazú', 'Jujuy',   
       'Junín', 'La Plata', 'La Rioja', 'Malargüe', 'Mar del Plata', 'Mendoza',
       'Moreno', 'Morón', 'Neuquén', 'Paraná', 'Paso de los Libres', 'Posadas',
       'Puerto Madryn', 'Reconquista', 'Resistencia', 'Río Cuarto',
       'Río Gallegos', 'Río Grande', 'Rosario', 'Salta', 'San Fernando',       
       'San Juan', 'San Luis', 'San Rafael', 'Santa Fe', 'Santa Rosa',
       'Santa Rosa de Conlara', 'Santiago del Estero', 'Tandil',
       'Termas Río Hondo', 'Trelew', 'Tucumán', 'Ushuaia', 'Viedma',
       'Villa Gesell', 'Villa Reynolds', 'Otros']
        for colum in colums: 
            df[colum] = df[colum].astype(float)
            df[colum] = df[colum]*1000
        print(df['Corrientes'])
        column_names = {
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
        # Corrige los nombres de las columnas en el DataFrame
        df.rename(columns=column_names, inplace=True)
        return df
        

    
class ArmadoDF:
    @staticmethod
    def buscar_fila_por_valor(df, target_value):
        # Buscar el valor en todas las columnas del DataFrame
        for index, row in df.iterrows():
            if target_value in row.values:
                return index  # Devolver el índice de la fila si se encuentra el valor
        return None  # Devolver None si el valor no se encuentra en ninguna fila
