import datetime
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import openpyxl
import pandas as pd
from datetime import datetime

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
        try:
            #ruta_archivo_excel = 'C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\script_DNRPA\\prueba.xlsx' 
            ruta_archivo_excel = 'D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\script_DNRPA\\prueba.xlsx'
            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ SELENIUM ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
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
            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ ENCONTRAR Y TOMAR LOS DATOS ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            # Encontrar el elemento <div> con la clase 'grid'
            elemento_div = driver.find_element(By.CLASS_NAME, 'grid')

            # Encontrar la tabla dentro del elemento <div>
            elemento_tabla = elemento_div.find_element(By.TAG_NAME, 'table')

            # Obtener todas las filas de la tabla
            filas = elemento_tabla.find_elements(By.TAG_NAME, 'tr')

            # Lista para almacenar los datos de la tabla
            tabla_datos = []
            
            for fila in filas:
                # Obtener las celdas de cada fila, excluyendo la última columna y la última celda de encabezado
                celdas = fila.find_elements(By.TAG_NAME, 'th') + fila.find_elements(By.TAG_NAME, 'td')[:-1]

                # Lista para almacenar los valores de cada fila
                fila_datos = []

                for celda in celdas:
                    valor = celda.text
                    if isinstance(valor, str):
                        # Verificar si el valor comienza con un número
                        if valor.strip() and valor[0].isdigit():
                            try:
                                # Reemplazar el punto decimal por una coma (si es necesario)
                                valor = valor.replace('.', '')
                                # Intentar convertir el valor a float
                                valor = float(valor)
                                print("valor1: ", valor)
                            except ValueError:
                                pass  # Mantener el valor original si no se puede convertir a float
                    fila_datos.append(valor)
                print("aca: ", fila_datos)
                # Verificar si la última celda es "Total" y eliminarla
                if fila_datos and fila_datos[-1] == "Total":
                    fila_datos.pop()
                
                if tabla_datos and tabla_datos[0][0] == 'Provincia / Mes':
                    # Obtener el año deseado
                    valor_deseado = '2014'

                    # Modificar los nombres de los meses en la primera lista
                    for i in range(1, len(tabla_datos[0])):
                        # Generar la fecha en formato 'YYYY-MM-DD' para el mes y año correspondientes
                        fecha_str = valor_deseado + '-' + format(i, '02d') + '-01'
                        fecha = datetime.strptime(fecha_str, '%Y-%m-%d').strftime('%Y-%m-%d')

                        # Reemplazar el valor en la primera lista con la fecha
                        tabla_datos[0][i] = fecha

                tabla_datos.append(fila_datos) 
            
            datos_sin_segunda_fila = tabla_datos[0:1] + tabla_datos[2:]
            # Transponer los datos utilizando pandas
            df = pd.DataFrame(datos_sin_segunda_fila)
            df_transpuesta = df.transpose()
            
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
            # Obtener el año deseado
            anio_deseado = valor_deseado

            # Guardar el archivo Excel actualizado sobreescribiendo los datos existentes
            libro_excel.save(ruta_archivo_excel)

            #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ MYSQL ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            column_mapping = {
                'Provincia / Mes': 'Fecha',
                'BUENOS AIRES': 'Bueos_Aires',
                'C.AUTONOMA DE BS.AS': 'C_Autonoma_De_BSAS',
                'CATAMARCA': 'Catamarca',
                'CORDOBA': 'Cordoba',
                'CORRIENTES': 'Corrientes',
                'CHACO': 'Chaco',
                'CHUBUT': 'Chubut',
                'ENTRE RIOS': 'Entre_Rios',
                'FORMOSA': 'Formosa',
                'JUJUY': 'Jujuy',
                'LA PAMPA': 'La_Pampa',
                'LA RIOJA': 'La_Rioja',
                'MENDOZA': 'Mendoza',
                'MISIONES': 'Misiones',
                'NEUQUEN': 'Neuquen',
                'RIO NEGRO': 'Rio_Negro',
                'SALTA': 'Salta',
                'SAN JUAN': 'San_Juan',
                'SAN LUIS': 'San_Luis',
                'SANTA CRUZ': 'Santa_Cruz',
                'SANTA FE': 'Santa_Fe',
                'SGO.DEL ESTERO': 'Sgo_Del_Estero',
                'TUCUMAN': 'Tucuman',
                'T.DEL FUEGO': 'Tierra_Del_Fuego',
                'TOTAL': 'Total_Nacion'
            }
            columnas_mysql = list(column_mapping.values())

            # Recorrer los datos transpuestos y escribirlos en la base de datos MySQL
            for valores_fila in valores_transpuestos:
                # Crear la sentencia SQL para la inserción
                sql = f"INSERT INTO dnrpa_nacion_auto ({', '.join(columnas_mysql)}) VALUES ({', '.join(['%s'] * len(columnas_mysql))})"
                
                # Obtener los valores correspondientes a las columnas de MySQL
                valores_mysql = [valores_fila[column_mapping[columna]] for columna in columnas_mysql]
                
                # Ejecutar la sentencia SQL con los valores
                conn.execute(sql, valores_mysql)
            # Confirmar los cambios en la base de datos
            conn.commit()
            
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
            
loadHTML_TablaAutoNacion().loadInDataBase(host, user, password, database)