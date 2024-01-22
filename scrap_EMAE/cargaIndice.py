import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import mysql
import mysql.connector
from dateutil.relativedelta import relativedelta
from email.message import EmailMessage
import ssl
import smtplib
import calendar
from informes import InformesEmae

class cargaIndice:

    def __init__(self):

        self.conn = None
        self.cursor = None 

    def conecta_bdd(self,host, user, password, database):

        # Conexión a la base de datos MySQL
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        self.cursor = self.conn.cursor()


        
    def loadXLSIndiceEMAE(self, file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database):



        # Leer el archivo Excel en un DataFrame de pandas
        df = pd.read_excel(file_path, sheet_name=0, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None
        # Eliminar la última fila que contiene "Fuente: INDEC"
        df = df.drop(df.index[-1])
        df = df.drop(df.index[-1])
        

        print(" \n DATAFRAME \n")

        #print(df)

        print("=========== \n\n")

        # Obtener las columnas desde C hasta R
        columnas_valores = df.columns[2:18]  # Columnas C a R

        print(" \n Columnas \n")

        lista_columnas = list(columnas_valores)

        #Lista de valores, contiene numeros del 1 al 16. Representan las categorias del EMAE.
        rango_columnas = range(1,len(columnas_valores) + 1)
        #print("Rango DE COLUMNAS", rango_columnas)


        print("=========== \n\n") 

        
        fecha_inicio = datetime(2003, 12, 1)
        num_meses = len(df) - 2  # Restar 2 para compensar las filas de encabezados

        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        # Iterar a través de las filas a partir de la fila 3
        for index, row in df.iterrows():
            if index >= 2:  # Fila 3 en adelante
                fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                for columna in columnas_valores:

                    #Buscamos el valor por FILA|COLUMNA y lo agregamos a la lista
                    valor = df.at[index, columna]
                    lista_valores.append(valor)
                    
                    #Buscamos el sector por FILA|COLUMNA y lo agregamos a la lista
                
                    sector_productivo = df.at[0, columna]  # Fila 1 (Fila de los años)

                    indice_sector_productivo = lista_columnas.index(columna) + 1

                    lista_SectorProductivo.append(indice_sector_productivo)
                    print(f"Fecha: {fecha}, Valor: {valor}, Sector Productivo: {indice_sector_productivo}")
        
        

        #Conectamos a la BDD 
        self.conecta_bdd(host, user, password, database)

        #Verificar cantidad de filas anteriores 
        select_row_count_query = "SELECT COUNT(*) FROM emae"
        self.cursor.execute(select_row_count_query)
        row_count_before = self.cursor.fetchone()[0]
        
        delete_query ="TRUNCATE `ipecd_economico`.`emae`"
        self.cursor.execute(delete_query)
        
        # Iterar a través de las filas a partir de la fila 3
        for index, row in df.iterrows():
            if index >= 3:  # Fila 3 en adelante
                fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                for columna in columnas_valores:

                    #Buscamos el valor por FILA|COLUMNA y lo agregamos a la lista
                    valor = df.at[index, columna]
                    indice_sector_productivo = lista_columnas.index(columna) + 1

                    # Insertar en la tabla MySQL
                    query = "INSERT INTO emae (Fecha, Sector_Productivo, Valor) VALUES (%s, %s, %s)"
                    values = (fecha, indice_sector_productivo, valor)
                    self.cursor.execute(query, values)

        self.conn.commit()

        
        self.cursor.execute(select_row_count_query)
        row_count_after = self.cursor.fetchone()[0]
        #Comparar la cantidad de antes y despues
        
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")

            InformesEmae(host, user, password, database).enviar_mensajes()
            #self.enviar_correo()   
        else:
            print("Se realizo una verificacion de la base de datos")
            
        
        self.conn.commit()
        # Cerrar la conexión a la base de datos
        self.cursor.close()
        self.conn.close()
            


    def enviar_correo(self):
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']
        asunto = 'Actualizacion de los datos del Estimador Mensual de Actividad Económico (EMAE)'


        df_mensual,df_interanual,df_acumulado,fecha_maxima = self.variaciones_mensual_interanual_acumulada()

        #Construimos la cadena de la fecha actual
        cadena_fecha_actual = self.obtener_fecha_actual(fecha_maxima)
    
        cadena_inicio = f'''

        <html>
        <body>

        <h3> ACTUALIZACION DE DATOS, se actualizaron los datos del Estimador Mensual de Actividad Económico(EMAE). Fecha: {cadena_fecha_actual} </h3>
        
        <hr>
        '''

        # Cabeza de tabla
        cabeza_tabla_variaciones = f'''

        <h3> Variaciones a nivel Nacional del Estimador Mensual de Actividad Económico (EMAE)- Argentina </h3>

        
        <table style="border-collapse: collapse; width: 100%;">

        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">  VAR. MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. ACUMULADA</th>
        '''


        #Datos correspondientes a cada variacion
        for nombre_mensual,var_mensual,nombre_interanual,var_interanual, nombre_acumulado,var_acumulada in zip(df_mensual['nombre_indices'],df_mensual['var_mensual'],df_interanual['nombre_indices'],df_interanual['var_interanual'],df_acumulado['nombre_indices'],df_acumulado['var_acumulada']):
        
            fila_de_nea = f'''
                <tr>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_mensual}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_mensual:.2f}%</td>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_interanual}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_interanual:.2f}%</td>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_acumulado}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_acumulada:.2f}%</td>
                </tr>
                '''

            cabeza_tabla_variaciones = cabeza_tabla_variaciones + fila_de_nea
    

        fin_mensaje = f'''
        </table> 


        <hr>

        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>

        </body>
        </html>
        '''



        mensaje_final = cadena_inicio + cabeza_tabla_variaciones + fin_mensaje



        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = email_receptores
        em['Subject'] = asunto
        em.set_content(mensaje_final, subtype = 'html')
        
        contexto= ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())



    def variaciones_mensual_interanual_acumulada(self):
        
        #Buscamos los datos de la tabla emae, y lo transformamos a un dataframe
        nombre_tabla = 'emae'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_bdd = pd.read_sql(query_select,self.conn)
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])#--> Cambiamos formato de la fecha para su manipulacion
        
        #Buscamos los datos de las categorias del emae, para lograr un for con cada indice, y para organizar la tabla por |INDICE|VALOR
        nombre_tabla = 'emae_categoria'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_categorias = pd.read_sql(query_select,self.conn)


        #OBTENCION DEL GRUPO DE LA FECHA MAXIMA 
        fecha_maxima = max(df_bdd['Fecha'])
        df_bdd_ultima_fecha = df_bdd[df_bdd['Fecha'] == fecha_maxima]


        #LISTAS que acumulan consecutivamente los indices y las variaciones
        lista_indices = []
        lista_var_mensual = []
        lista_var_interanual = []
        lista_var_acumulada = []

        for indice,descripcion_categoria in zip(df_categorias['id_categoria'],df_categorias['categoria_descripcion']):


            #Obtenemos el valor actual y el valor del mes anterior de la misma categoria --> SE USA EL MISMO VALOR PARA CADA VARIACION
            valor_actual = df_bdd_ultima_fecha['Valor'][df_bdd_ultima_fecha['Sector_Productivo'] == indice].values[0]

            # === CALCULO VARIACION MENSUAL

            valor_mes_anterior = df_bdd['Valor'][ (df_bdd['Fecha'].dt.year == fecha_maxima.year)
                                                & (df_bdd['Fecha'].dt.month == fecha_maxima.month - 1)
                                                & (df_bdd['Sector_Productivo'] == indice)].values[0]

            #Calculo final
            var_mensual = ((valor_actual / valor_mes_anterior) - 1) * 100

            # === CALCULO VARIACION INTERANUAL
            
            valor_año_anterior = df_bdd['Valor'][ (df_bdd['Fecha'].dt.year == fecha_maxima.year - 1)
                                                & (df_bdd['Fecha'].dt.month == fecha_maxima.month)
                                                & (df_bdd['Sector_Productivo'] == indice)].values[0]

            #Calculo final
            var_intearnual = ((valor_actual / valor_año_anterior) - 1) * 100

            # === CALCULO VARIACION ACUMULADA    
            valor_diciembre_año_anterior = df_bdd['Valor'][ (df_bdd['Fecha'].dt.year == fecha_maxima.year - 1)
                                                & (df_bdd['Fecha'].dt.month == 12)
                                                & (df_bdd['Sector_Productivo'] == indice)].values[0]

            #Calculo final
            var_acumulada = ((valor_actual / valor_diciembre_año_anterior) - 1) * 100



            #Agregamos cada valor a su correspondiente lista
            lista_indices.append(descripcion_categoria)
            lista_var_mensual.append(var_mensual)
            lista_var_interanual.append(var_intearnual)
            lista_var_acumulada.append(var_acumulada)


        

        #Creacion de DATAFRAME
        data = {
                
                'nombre_indices':lista_indices,
                'var_mensual': lista_var_mensual,
                'var_interanual':lista_var_interanual,
                'var_acumulada':lista_var_acumulada
        }

        df = pd.DataFrame(data)

        
        #Ordenacion por var mensual | intearanual| acumulado
        df_mensual = df.sort_values(by='var_mensual',ascending=[False])
        df_interanual = df.sort_values(by='var_interanual',ascending=[False])
        df_acumulado = df.sort_values(by='var_acumulada',ascending=[False])
        
        return df_mensual,df_interanual,df_acumulado, fecha_maxima


    
    def obtener_fecha_actual(self,fecha_ultimo_registro):
        

        # Obtener el nombre del mes actual en inglés
        nombre_mes_ingles = calendar.month_name[fecha_ultimo_registro.month]

        # Diccionario de traducción
        traducciones_meses = {
            'January': 'Enero',
            'February': 'Febrero',
            'March': 'Marzo',
            'April': 'Abril',
            'May': 'Mayo',
            'June': 'Junio',
            'July': 'Julio',
            'August': 'Agosto',
            'September': 'Septiembre',
            'October': 'Octubre',
            'November': 'Noviembre',
            'December': 'Diciembre'
        }

        # Obtener la traducción
        nombre_mes_espanol = traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)

        return str(fecha_ultimo_registro.day) + f" de {nombre_mes_espanol} del {fecha_ultimo_registro.year}"

