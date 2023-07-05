import datetime
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from tabulate import tabulate
import openpyxl
import pandas as pd
from dateutil.parser import parse
import re

host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

class loadHTML_TablaAutoNacion:
    def loadInDataBase(self, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            ruta_archivo_excel = 'C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\script_DNRPA\\prueba.xlsx' 
            #ruta_archivo_excel = 'D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\script_DNRPA\\prueba.xlsx'
            driver = webdriver.Chrome()
            driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

            # Obtener la ventana actual
            ventana_actual = driver.current_window_handle
            
            elemento = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')
            # Obtener todas las opciones del elemento select
            opciones = elemento.find_elements(By.TAG_NAME, 'option')

            # Buscar la opción deseada por su valor y hacer clic en ella
            valor_deseado = '2014'  # Valor de la opción que deseas seleccionar

            for opcion in opciones:
                if opcion.get_attribute('value') == valor_deseado:
                    opcion.click()
                    break
            
            boton = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[4]/td/input[1]')
            boton.click()
            
            time.sleep(5)
            
            boton_aceptar = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/center/input')
            boton_aceptar.click()
            
            # Esperar un momento para que se abra la nueva pestaña
            driver.implicitly_wait(5)
            # Cambiar al contexto de la nueva pestaña
            for ventana in driver.window_handles:
                if ventana != ventana_actual:
                    driver.switch_to.window(ventana)
            
            time.sleep(5)

            # Encontrar el elemento <div> con la clase 'grid'
            elemento_div = driver.find_element(By.CLASS_NAME, 'grid')

            # Encontrar la tabla dentro del elemento <div>
            elemento_tabla = elemento_div.find_element(By.TAG_NAME, 'table')

            # Obtener todas las filas de la tabla
            filas = elemento_tabla.find_elements(By.TAG_NAME, 'tr')

            # Lista para almacenar los datos de la tabla
            tabla_datos = []

            for fila in filas:
                # Obtener las celdas de cada fila
                celdas = fila.find_elements(By.TAG_NAME, 'th') + fila.find_elements(By.TAG_NAME, 'td')

                # Lista para almacenar los valores de cada fila
                fila_datos = []

                # Recorrer las celdas de la fila
                for celda in celdas:
                    valor = celda.text
                    if isinstance(valor, str):
                        # Verificar si el valor comienza con un número
                        if valor.strip() and valor[0].isdigit():
                            try:
                                # Intentar convertir el valor a float
                                valor = float(valor)
                                print("valor1: ", valor)
                            except ValueError:
                                pass  # Mantener el valor original si no se puede convertir a float
                    fila_datos.append(valor)

                # Agregar la lista de datos de la fila a la tabla de datos
                tabla_datos.append(fila_datos)           
            
            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓FUNCIONA BIEN MODIFICAR LO DE ABAJO ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            datos_sin_segunda_fila = tabla_datos[0:1] + tabla_datos[2:]
            # Transponer los datos utilizando pandas
            df = pd.DataFrame(datos_sin_segunda_fila)
            df_transpuesta = df.transpose()


            # Convertir los valores de la transposición a una lista
            valores_transpuestos = df_transpuesta.values.tolist()

            # Cargar el archivo Excel existente
            libro_excel = openpyxl.load_workbook(ruta_archivo_excel)

           # Seleccionar la hoja activa del libro
            hoja_activa = libro_excel.active

            # Obtener la última fila existente en el archivo
            ultima_fila = hoja_activa.max_row + 1

            # Recorrer los datos transpuestos y escribirlos en el archivo de Excel
            for fila_datos in valores_transpuestos:
                hoja_activa.append(fila_datos)
                
            df_transpuesta.drop
            
            # Guardar el archivo Excel actualizado sobreescribiendo los datos existentes
            libro_excel.save(ruta_archivo_excel)

            # Se toma el tiempo de finalización y se calcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de SIPA NACIONAL CON ESTACIONALIDAD")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            
loadHTML_TablaAutoNacion().loadInDataBase(host, user, password, database)