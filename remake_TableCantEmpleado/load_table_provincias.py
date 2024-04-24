import mysql
import mysql.connector


class load_table_provincia:
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
    
    def read_database(self):
        from datetime import date, timedelta
        id_provincia = 2
        start_date = date(2010, 1, 1)
        end_date = date(2025, 1, 1)
        # Generate a list of dates from start_date to end_date yearly on January 1st
        date_list = [start_date]
        current_date = start_date
        while current_date.year < end_date.year:
            current_date = current_date.replace(year=current_date.year + 1)
            date_list.append(current_date)
        results = {}
        
        # Consulta para obtener las fechas disponibles en sipa_registro para la provincia
        query_fechas_sipa = """
        SELECT fecha FROM ipecd_economico.sipa_registro
        WHERE id_provincia = %s
        ORDER BY fecha DESC;
        """
        self.cursor.execute(query_fechas_sipa, (id_provincia,))
        fechas_sipa = [row[0] for row in self.cursor.fetchall()]
        
        for date in date_list:
            # Encuentra la fecha más cercana al inicio del año en sipa_registro
            sipa_date = max(fecha for fecha in fechas_sipa if fecha < date)
            
            # Consulta para obtener la población total
            query_poblacion = """
            SELECT SUM(Poblacion) FROM ipecd_economico.censo_provincia
            WHERE ID_Provincia = %s AND fecha = %s;
            """
            self.cursor.execute(query_poblacion, (id_provincia, date.strftime('%Y-%m-%d')))
            result_poblacion = self.cursor.fetchone()
            total_poblacion = result_poblacion[0] if result_poblacion[0] is not None else 0

            # Consulta para obtener el total de empleo sin estacionalidad de la fecha correspondiente en sipa_registro
            query_empleo = """
            SELECT SUM(cantidad_sin_estacionalidad) FROM ipecd_economico.sipa_registro
            WHERE id_provincia = %s AND fecha = %s;
            """
            self.cursor.execute(query_empleo, (id_provincia, sipa_date))
            result_empleo = self.cursor.fetchone()
            total_empleo = result_empleo[0] if result_empleo[0] is not None else 0

            if total_poblacion > 0:  # Evitar división por cero
                empleo_por_mil = ((total_empleo *1000) / total_poblacion) * 1000
            else:
                empleo_por_mil = 0
            results[date.strftime('%Y')] = empleo_por_mil

        return results
                    