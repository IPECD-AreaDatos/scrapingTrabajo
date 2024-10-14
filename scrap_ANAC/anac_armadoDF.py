import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

class armadoDF:
    @staticmethod
    def armado_df(file_path, target_value="TABLA 11"):
        """
        Función para leer un archivo xlsx y armar un DataFrame con la información
        necesaria para la carga en la base de datos.

        Parameters
        ----------
        file_path : str
            Ruta del archivo xlsx a leer
        target_value : str
            Valor para buscar en el archivo

        Returns
        -------
        pd.DataFrame
            DataFrame con la información necesaria para la carga en la base de datos
        """
        try:
            df = pd.read_excel(file_path, sheet_name=0)
        except Exception as e:
            print(f"Error al leer el archivo: {e}")
            return None

        fila_target = armadoDF.buscar_fila_por_valor(df, target_value)
        if fila_target is None:
            print(f"No se encontró el valor '{target_value}' en el archivo.")
            return None
        
        # Seleccionar las filas necesarias
        fila_inicio = fila_target + 2
        df = df.iloc[fila_inicio:(fila_inicio + 58)]
        
        # Borrar columnas que tienen solo valores None
        df.dropna(axis=1, how='all', inplace=True)

        # Transponer y renombrar columnas con los valores de la primera fila
        df = df.transpose()
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        
        # Crear la nueva columna con las fechas
        fecha_inicio = datetime.strptime("2019-01-01", "%Y-%m-%d").date()
        fechas = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        df.insert(0, 'fecha', fechas)
        
        # Eliminar la última columna (si es necesario)
        if df.columns[-1] is None:
            df.drop(df.columns[-1], axis=1, inplace=True)

        # Convertir columnas a float y multiplicar por 1000
        columnas_a_convertir = ['Aeroparque', 'Bahía Blanca', 'Bariloche', 'Base Marambio',    
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

        for col in columnas_a_convertir:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce') * 1000

        # Renombrar las columnas con el diccionario especificado
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

        df.rename(columns=column_names, inplace=True)
        return df

    @staticmethod
    def buscar_fila_por_valor(df, target_value):
        """Busca el valor en todas las columnas del DataFrame y devuelve el índice de la fila si se encuentra el valor."""
        try:
            for index, row in df.iterrows():
                if target_value in row.values:
                    return index  # Devolver el índice de la fila si se encuentra el valor
        except Exception as e:
            print(f"Error durante la búsqueda del valor: {e}")
        return None  # Devolver None si el valor no se encuentra en ninguna fila
