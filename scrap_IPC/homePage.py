import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

class HomePage:
    
    def __init__(self):
        
        #Configuracion del navegador
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome(options=options)

        #URL DE LA SECCION DE IPC, FUENTE:INDEC
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-5-31'

    def descargar_archivo(self):

        
        #==== CARGAMOS LA PAGINA

         # Cargar la página web
        self.driver.get(self.url_pagina)

        #esperamos 20 segs.
        wait = WebDriverWait(self.driver, 20)
        

        #==== CONSTRUCCION DE RUTAS

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)


        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ PRIMER ARCHIVO  ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

        """
        Este archivo contiene las Variaciones mensuales NACIONALES, divido por CATEGORIA. Tambien los valores de la SERIE ORIGINAL.
        (Tambien tiene las Var. por categoria de todas las regiones, a nosotros solo los interesa los datos nacionales).

        Generalmente, el nombre con el que aparece es: 
        'Índices y variaciones porcentuales mensuales e interanuales según divisiones de la canasta, bienes y servicios,
        clasificación de grupos. Diciembre de 2016- 'MES' de 'AÑO MAXIMO DEL ARCHIVO'

        Al descargar el archivo, su nombre sera: 'sh_ipc_{ultimo_mes_publicado}_{año_del_ultimo_mes_publicado}'.xls

        """

        #Definimos XPATH
        xpath_archivo_sh_ipc_mes_ano = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[1]/div[2]/div/div/a"

        #Ubicamos el archivo con el XPATH
        archivo_sh_ipc_mes_ano = wait.until(EC.presence_of_element_located((By.XPATH, xpath_archivo_sh_ipc_mes_ano )))

        time.sleep(5)

        #Extraemos atributo HREF del elemento HTML
        url_archivo_categoria = archivo_sh_ipc_mes_ano.get_attribute('href')

        #Nombre del archivo
        nombre_archivo_categoria = 'sh_ipc_mes_ano.xls'

        # Descargar el segundo archivo desactivando la verificación del certificado SSL
        response = requests.get(url_archivo_categoria, verify=False)

        # Guardar el segundo archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo_categoria)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)    



        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ SEGUNDO ARCHIVO ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

        """
        Este archivo contiene:
            * Vars. mensuales de las CATEGORIAS, de las, DIVISIONES, y de las SUBDIVICIONES. DIVIDO POR REGION.
            * Tambien obtenemos los datos de la serie original.

        El nombre con el que aparece, generalmente es:

        Índices y variaciones porcentuales mensuales e interanuales según principales aperturas de la canasta. Diciembre de 2016- 'MES ULTIMO ANO' de 'ANO DEL ULTIMO MES'

        El nombre que aparece al descargar el archivo es: 'sh_ipc_aperturas.xls'

        """

        xpath_ipc_aperturas = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[3]/div[2]/div/div/a"

        # Esperar hasta que aparezca el enlace al primer archivo
        archivo_ipc_aperturas = wait.until(EC.presence_of_element_located((By.XPATH, xpath_ipc_aperturas )))
        
        #Obtenemos el atributo HREF del elemento HTML
        url_archivo_SP = archivo_ipc_aperturas.get_attribute('href')

        # Nombre del primer archivo
        nombre_archivo_SP = 'sh_ipc_aperturas.xls'

        # Descargar el primer archivo
        response = requests.get(url_archivo_SP, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo_SP)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)
        
        
        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ TERCER ARCHIVO  ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

        """
        Este archivo es el de los productos de IPC, contiene los precios 
        promedios de los productos, divido por REGION.

        Su nombre generalmente es:

        ' Índice de precios al consumidor. Precios promedio de un conjunto de elementos de la canasta del IPC,
          según regiones (Enero de 2017- 'MES' de 'ultimo_ano') y para el GBA (Abril de 2016-junio de 2024) '


        Al descargar el archivo, su nombre sera: 'sh_ipc_precios_promedio.xls' 

        """

        #Definimos xpath
        xpath_archivo_precios_promedios = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[5]/div[2]/div/div/a"

        #Obtencion del XPATH - Se usara para obtener el HREF
        archivo_precios_promedios = wait.until(EC.presence_of_element_located((By.XPATH, xpath_archivo_precios_promedios )))

        time.sleep(5)

        #Obtencion del atributo HREF
        url_archivo_precios_promedios = archivo_precios_promedios.get_attribute('href')

        #Nombre que pondremos al archivo
        nombre_archivo_precios_promedios = 'sh_ipc_precios_promedio.xls'

        # Descargar el segundo archivo desactivando la verificación del certificado SSL
        response = requests.get(url_archivo_precios_promedios, verify=False)

        #guardamos 'sh_ipc_promedios.xls' en la carpeta files
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo_precios_promedios)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)


        # Cerrar el navegador
        self.driver.quit()
