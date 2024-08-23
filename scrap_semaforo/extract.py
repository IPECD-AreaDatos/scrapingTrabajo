import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pandas import DataFrame
from dotenv import load_dotenv
from json import loads

class ExtractSheet: 
    def extract_sheet(self):

        # Cargar las variables de entorno desde el archivo .env
        load_dotenv()


        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1HnK6eMrd_P6V8P141WPZ0jz2ivoO6opyX0YWy342fRM'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()


        """
        Vamos a dividir la llamada en 2 parte:

        1 - La primera para detectar que fila tiene mas datos (sera el largo a tomar)
        2 - Tomar los datos con el largo definido
        """

        # === PRIMERA PARTE

        #Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'B2:12'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Hoja 1!B2:12').execute()

        # Extrae los valores del resultado - Es una lista, compuesta por las listas de valores
        filas_con_valores = result.get('values', [])[1:]
        
        #Vamos a iterar cada vector, buscando el de mayor tamaño
        max_length = 0 #--> Bandera de tamaño

        # Iterar sobre cada lista y comparar su longitud
        for sublist in filas_con_valores:
            if len(sublist) > max_length:
                max_length = len(sublist) #--> Obtener la lista de mayor tamaño en cada corrida


        #=== SEGUNDA PARTE
        lista_sin_nulos = self.anadir_nulos(filas_con_valores,max_length)

        #=== OBTENCION DE FECHAS

        #Buscamos la fila de las fechas y obtenemos un diccionario
        fila_fechas = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Hoja 1!B2:2').execute()

        #Buscamos la key 'values' y el elemento 0, ya que las fechas es una lista, dentro de otra lista
        valores_fecha = fila_fechas['values'][0]

        #Se toma cierta cantidad de fechas, definido por la fila que presenta mayor cantidad de datos
        fechas_truncadas = valores_fecha[0:max_length]


        #=== CONSTRUCCION DEL DATAFRAME

        df_semaforo = DataFrame()
        df_semaforo['fecha'] = fechas_truncadas
        df_semaforo['combustible_vendido'] = lista_sin_nulos[0]
        df_semaforo['patentamiento_0km_auto'] = lista_sin_nulos[1]
        df_semaforo['patentamiento_0km_motocicleta'] = lista_sin_nulos[2]
        df_semaforo['pasajeros_salidos_terminal_corrientes'] = lista_sin_nulos[3]
        df_semaforo['pasajeros_aeropuesto_corrientes'] = lista_sin_nulos[4]
        df_semaforo['venta_supermercados_autoservicios_mayoristas'] = lista_sin_nulos[5]
        df_semaforo['exportaciones_aduana_corrientes_dolares'] = lista_sin_nulos[6]
        df_semaforo['exportaciones_aduana_corrientes_toneladas'] = lista_sin_nulos[7]
        df_semaforo['empleo_privado_registrado_sipa'] = lista_sin_nulos[8]
        df_semaforo['ipicorr'] = lista_sin_nulos[9]

        return df_semaforo


    def anadir_nulos(self,filas_con_valores,max_length):

        listas_con_nulos = []

        #Vamos a sacar las diferencias de tamaño, para saber cuantos nulos agregar a cada lista
        for fila in filas_con_valores:

            tamano_lista = len(fila)
            diferencia = max_length - tamano_lista

            #No hay valores nulos para agregar
            if diferencia == 0:
                listas_con_nulos.append(fila)

            #Añadimos nulos segun la diferencia
            else:

                contador_de_nulos_anadidos = 0 #--> Contador de nulos que se añaden

                #Mientras el contador, no llegue a la diferencia marcada, seguir agregando nulos
                while contador_de_nulos_anadidos < diferencia:
            
                    fila.append(None)
                    contador_de_nulos_anadidos = contador_de_nulos_anadidos + 1

                listas_con_nulos.append(fila)

        return listas_con_nulos
        

