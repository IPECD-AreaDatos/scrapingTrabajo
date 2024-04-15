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
        from datetime import date
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
        for date in date_list:
            query = """
            SELECT SUM(Poblacion) FROM ipecd_economico.censo_provincia
            WHERE ID_Provincia = %s AND fecha = %s;
            """
            self.cursor.execute(query, (id_provincia, date.strftime('%Y-%m-%d')))
            result = self.cursor.fetchone()  # fetchone() because SUM returns a single row with the sum value

            # Fetchone returns a tuple, and since we're summing one column, we access the first element.
            total_sum = result[0] if result[0] is not None else 0
            results[date.strftime('%Y-%m-%d')] = total_sum

        return results
                