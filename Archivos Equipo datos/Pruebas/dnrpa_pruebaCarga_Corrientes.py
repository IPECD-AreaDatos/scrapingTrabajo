import datetime
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import openpyxl
import pandas as pd
from datetime import datetime
import os

host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

class loadHTML_TablaAutoInscripcionCorrientes:
    def loadInDataBase(self, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        try:

            # Obtener la ruta del directorio actual (donde se encuentra el script)
            directorio_actual = os.path.dirname(os.path.abspath(__file__))
            ruta_carpeta_files = os.path.join(directorio_actual, 'files')
            ruta_archivo_excel = os.path.join(ruta_carpeta_files, 'pruebaReestructuracion.xlsx')

            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ SELENIUM ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            driver = webdriver.Chrome()
            driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

            # Obtener la ventana actual
            ventana_actual = driver.current_window_handle
            
            elemento = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')
            # Obtener todas las opciones del elemento select
            opciones = elemento.find_elements(By.TAG_NAME, 'option')

            # Buscar la opción deseada por su valor y hacer clic en ella
            valor_deseado = '2023'  # Valor de la opción que deseas seleccionar

            for opcion in opciones:
                if opcion.get_attribute('value') == valor_deseado:
                    opcion.click()
                    break
            
            boton = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[4]/td/input[1]')
            boton.click()
            
            time.sleep(1)
            
            boton_aceptar = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/center/input')
            boton_aceptar.click()
            
            # Esperar un momento para que se abra la nueva pestaña
            driver.implicitly_wait(1)
            # Cambiar al contexto de la nueva pestaña
            for ventana in driver.window_handles:
                if ventana != ventana_actual:
                    driver.switch_to.window(ventana)
            
            time.sleep(1)
            
            # Encontrar el elemento del enlace por el texto visible completo
            enlace = driver.find_element(By.LINK_TEXT, "CORRIENTES")

            # O encontrar el elemento del enlace por el texto visible parcial
            # enlace = driver.find_element_by_partial_link_text("CORRIENTES")

            # Hacer clic en el enlace
            enlace.click()
            # Esperar un momento para que se abra la nueva pestaña
            driver.implicitly_wait(1)
            # Cambiar al contexto de la nueva pestaña
            for ventana in driver.window_handles:
                if ventana != ventana_actual:
                    driver.switch_to.window(ventana)
            
            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ ENCONTRAR Y TOMAR LOS DATOS ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            # Encontrar el elemento <div> con la clase 'grid'
            elemento_div = driver.find_element(By.CLASS_NAME, 'grid')

            # Encontrar la tabla dentro del elemento <div>
            elemento_tabla = elemento_div.find_element(By.TAG_NAME, 'table')

            # Obtener todas las filas de la tabla
            filas = elemento_tabla.find_elements(By.TAG_NAME, 'tr')

            # Obtener las celdas de la primera columna de la tabla
            regiones = driver.find_elements(By.XPATH, "//table//tr//td[1]")

            # Lista para almacenar los valores de la primera columna
            valores_regiones = [celda.text for celda in regiones]
            print("columna ----> ", valores_regiones)

            # Ahora puedes iterar por las filas y celdas de la tabla
            tabla_datos = []
            cantidad = []  # Tabla con todos los valores
            id_vehiculo = []
            id_provincia = [2, 6, 10, 22, 26, 14, 18, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 94, 90]
            i = 0
            
            for fila in filas:
                # Obtener las celdas de cada fila, excluyendo la primera columna y la última celda de encabezado
                celdas = fila.find_elements(By.TAG_NAME, 'td')[1:-1]

                # Lista para almacenar los valores de cada fila
                fila_datos = []

                for celda in celdas:
                    valor = celda.text
                    id_vehiculo.append(1)
                    if isinstance(valor, str):
                        # Verificar si el valor comienza con un número
                        if valor.strip() and valor[0].isdigit():
                            try:
                                # Reemplazar el punto decimal por una coma (si es necesario)
                                valor = valor.replace('.', '')
                                # Intentar convertir el valor a float
                                valor = int(valor)
                                print("valor1: ", valor)
                                cantidad.append(valor)  # ----> Tabla con todos los valores
                            except ValueError:
                                pass  # Mantener el valor original si no se puede convertir a float

                    fila_datos.append(valor)
                
                print("id_vehiculo ----> ", id_vehiculo)  # Imprimir el último valor agregado a id_vehiculo
                print("aca?: ", fila_datos)
                # Verificar si la última celda es "Total" y eliminarla
                if fila_datos and fila_datos[-1] == "Total":
                    fila_datos.pop()

                tabla_datos.append(fila_datos)
    
            datos_sin_segunda_fila = tabla_datos[0:1] + tabla_datos[2:]
            # Transponer los datos utilizando pandas
            df = pd.DataFrame(datos_sin_segunda_fila)
            print(df)
            df_transpuesta = df.transpose()
            df_transpuesta = df_transpuesta.drop(df_transpuesta.index[-1])
            df_transpuesta = df_transpuesta.drop(df_transpuesta.columns[-1],axis=1)
            
            
            #Conversion de MESES a formato Y-M-D , tipo de dato: datetime
            print(df_transpuesta[0][1:])
            meses = df_transpuesta[0][1:]

            #Donde almacenamos las nuevas fechas
            nuevas_fechas = list()

            for i in range(1, len(meses)+1):

                if i < 10:
                    fecha_str =  '01-0'+str(i)+"-"+ str(valor_deseado)
                else:
                    fecha_str = '01-'+str(i)+"-"+ str(valor_deseado)

                fecha_str = datetime.strptime(fecha_str,'%d-%m-%Y').date()
                nuevas_fechas.append(fecha_str)

            #Reasignacion de fechas
            df_transpuesta[0][1:] = nuevas_fechas
            
            # Convertir los valores de la transposición a una lista
            valores_transpuestos = df_transpuesta.values.tolist()
            
        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ EXCEL ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            # Cargar el archivo Excel existente
            libro_excel = openpyxl.load_workbook(ruta_archivo_excel)

           # Seleccionar la hoja activa del libro
            hoja_activa = libro_excel.active

            # Obtener la cantidad total de filas en el archivo
            total_filas = hoja_activa.max_row

            # Eliminar todas las filas excepto la primera (encabezados)
            hoja_activa.delete_rows(1, total_filas)
            
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
            print("Se guardaron los datos de Registro automotor")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Registro automotor: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error

loadHTML_TablaAutoInscripcionCorrientes().loadInDataBase(host, user, password, database)