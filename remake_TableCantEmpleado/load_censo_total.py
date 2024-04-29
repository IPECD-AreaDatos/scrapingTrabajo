import mysql
import mysql.connector
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
        self.conn = mysql.connector.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def read_censo(self, start_year, end_year):
        # Inicializar una lista vacía para almacenar los datos
        census_data = []
        
        # Generar una lista de fechas desde el año de inicio hasta el año de fin
        date_list = [date(year, 1, 1) for year in range(start_year, end_year + 1)]
        
        for current_date in date_list:
            # Consulta para obtener la población total de todas las provincias en la fecha actual
            query_poblacion = """
            SELECT ID_Provincia, SUM(Poblacion) AS Poblacion
            FROM ipecd_economico.censo_provincia
            WHERE fecha = %s
            GROUP BY ID_Provincia;
            """
            self.cursor.execute(query_poblacion, (current_date.strftime('%Y-%m-%d'),))
            # Fetch all results and append them to the census_data list
            census_data.extend([
                {'ID_Provincia': row[0], 'Poblacion': row[1], 'Fecha': current_date}
                for row in self.cursor.fetchall()
            ])
        
        # Convertir la lista de datos en un DataFrame
        df_census = pd.DataFrame(census_data)
        print(df_census[df_census['ID_Provincia'] == 10])
        return df_census
