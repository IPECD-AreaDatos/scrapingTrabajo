from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import pymysql

class readDataDolarBlue:
    def __init__(self, host, user, password, database, table_name):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name

    def readDataWebPage(self):
        self.url = "https://www.dolarito.ar/"
        #options = webdriver.ChromeOptions()
        #options.add_argument('headless')
        
        # Inicializar el driver con las opciones
        self.driver = webdriver.Chrome()

        try:
            # Cargar la página web
            self.driver.get(self.url)
            self.driver.implicitly_wait(20)
            # Tomar los datos del xpath
            dolar_blue_venta = self.driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[3]/ul/li[3]/div/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/p').text
            dolar_blue_compra = self.driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[3]/ul/li[2]/div/div[2]/div[3]/div/div[2]/div/div[2]/div[2]/p').text
            fecha_actual = datetime.now().strftime("%d/%m/%Y")

            # Dataframe correspondiente a los datos del dólar
            dataframe_dolar = pd.DataFrame(columns=['fecha', 'compra', 'venta'])
            dataframe_dolar.loc[0] = [fecha_actual, dolar_blue_venta, dolar_blue_compra]

            # Conversión de datos
            dataframe_dolar['fecha'] = pd.to_datetime(dataframe_dolar['fecha'], format="%d/%m/%Y")
            dataframe_dolar['compra'] = pd.to_numeric(dataframe_dolar['compra'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
            dataframe_dolar['venta'] = pd.to_numeric(dataframe_dolar['venta'].str.replace('.', '').str.replace(',', '.'), errors='coerce')

            print(dataframe_dolar)
        finally:
            # Cerrar el navegador
            self.driver.quit()

        return dataframe_dolar

    def insertDataFrameInDataBase(self, dataframe_dolar):
        # Conexión a la base de datos usando pymysql
        connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                fecha = dataframe_dolar['fecha'][0]

                # Verificar si la fecha ya existe
                query = f"SELECT COUNT(*) as count FROM {self.table_name} WHERE fecha = %s"
                cursor.execute(query, (fecha,))
                result = cursor.fetchone()
                count = result['count']

                # Si ya existe, eliminar la fila existente
                if count != 0:
                    delete_query = f"DELETE FROM {self.table_name} WHERE fecha = %s"
                    cursor.execute(delete_query, (fecha,))
                
                # Insertar la nueva fila
                insert_query = f"INSERT INTO {self.table_name} (fecha, compra, venta) VALUES (%s, %s, %s)"
                cursor.execute(insert_query, (fecha, dataframe_dolar['compra'][0], dataframe_dolar['venta'][0]))

            connection.commit()
        finally:
            connection.close()


