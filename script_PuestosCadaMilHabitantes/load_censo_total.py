import pymysql
import pandas as pd
from datetime import date

class load_censo_total:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def read_censo(self, start_year, end_year):
        # Inicializar una lista vacía para almacenar los datos
        dato_censo = []
        
        # Generar una lista de fechas desde el año de inicio hasta el año de fin
        date_list = [date(year, 1, 1) for year in range(start_year, end_year + 1)]
        
        for current_date in date_list:
            # Consulta para obtener la población total de todas las provincias en la fecha actual
            query_poblacion = """
            SELECT id_provincia, poblacion
            FROM datalake_sociodemografico.censo_estimado
            WHERE fecha = %s AND id_provincia = id_departamento;
            """
            self.cursor.execute(query_poblacion, (current_date.strftime('%Y-%m-%d'),))
            # Fetch all results and append them to the dato_censo list
            dato_censo.extend([
                {'id_provincia': row[0], 'poblacion': row[1], 'fecha': current_date}
                for row in self.cursor.fetchall()
            ])
        
        # Convertir la lista de datos en un DataFrame
        df_censo = pd.DataFrame(dato_censo)
        return df_censo