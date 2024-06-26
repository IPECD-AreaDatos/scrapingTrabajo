# -*- coding: utf-8 -*-
#Archivo destino a la construccion de informes que se envian por Whatsapp y Gmail.
import calendar
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
from datetime import datetime
from ssl import create_default_context
#from pywhatkit import sendwhatmsg_to_group_instantly
import mysql.connector

class InformesEmae:

    def __init__(self,host,user,password,database):

        #Declaramos las variables
        self.conn = None
        self.cursor = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Cuando se instancia la clase ya se conecta a la BDD para construir automaticamente los informes
        self.conectar_bdd()

    #Establecemos la conexion a la BDD
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    #Enviamos los mensajes a cada plataforma
    def enviar_mensajes(self):
        
        #Obtencion de datos
        df_mensual,df_interanual,df_acumulado,fecha_maxima = self.variaciones_mensual_interanual_acumulada()


        #Envio de mensajes
        self.enviar_correo(df_mensual,df_interanual,df_acumulado,fecha_maxima)
    
    #Construccion de correo a mandar
    def enviar_correo(self,df_mensual,df_interanual,df_acumulado,fecha_maxima):
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        
        #Construimos la cadena de la fecha actual
        cadena_fecha_actual = self.obtener_fecha_actual(fecha_maxima)

        asunto = f'Actualizacion de datos EMAE - {cadena_fecha_actual}'
        datos_ultimos, datos_penultimos = self.obtener_variacion_anualymensual()
        # Verificar que los datos no sean None antes de desempaquetar
        if datos_ultimos and datos_penultimos:
            # Desempaquetar los datos
            fecha_ultima, variacion_interanual_ultima, variacion_mensual_ultima = datos_ultimos
            fecha_penultima, variacion_interanual_penultima, variacion_mensual_penultima = datos_penultimos

            cadena_inicio = f'''
            <html>
            <body>
            <h2 style="font-size: 24px;"><strong> DATOS ACTUALIZADOS DEL ESTIMADOR MENSUAL DE ACTIVIDAD ECONOMICO (EMAE) A {cadena_fecha_actual.upper()}. </strong></h2>
            <h3 style="font-size: 19px;">Variación Mensual Desestacionalizada: {variacion_mensual_ultima:.1f}%<br>
            Variación Interanual: {variacion_interanual_ultima:.1f}%</h3>
            <hr>
            '''
            
            # Cabeza de tabla
            cabeza_tabla_variaciones = f'''
            <h3> Variaciones a nivel Nacional del Estimador Mensual de Actividad Económico (EMAE) - Argentina </h3>
            <table style="border-collapse: collapse; width: 100%;">
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">  VAR. MENSUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. INTERANUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. ACUMULADA</th>
            '''

            # Datos correspondientes a cada variacion
            for nombre_mensual, var_mensual, nombre_interanual, var_interanual, nombre_acumulado, var_acumulada in zip(df_mensual['nombre_indices'], df_mensual['var_mensual'], df_interanual['nombre_indices'], df_interanual['var_interanual'], df_acumulado['nombre_indices'], df_acumulado['var_acumulada']):
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
            em.set_content(mensaje_final, subtype='html')

            contexto = create_default_context()
            with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
                smtp.login(email_emisor, email_contraseña)
                smtp.sendmail(email_emisor, email_receptores, em.as_string())
        else:
            print("No hay suficientes registros para desempaquetar los datos.")


    def variaciones_mensual_interanual_acumulada(self):
        
        #Buscamos los datos de la tabla emae, y lo transformamos a un dataframe
        nombre_tabla = 'emae'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_bdd = pd.read_sql(query_select,self.conn)
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])#--> Cambiamos formato de la fecha para su manipulacion
        
        #Buscamos los datos de las categorias del emae, para lograr un for con cada indice, y para organizar la tabla por |INDICE|VALOR
        nombre_tabla = 'emae_categoria'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_categorias = pd.read_sql(query_select,self.conn)


        #OBTENCION DEL GRUPO DE LA FECHA MAXIMA 
        fecha_maxima = max(df_bdd['fecha'])
        df_bdd_ultima_fecha = df_bdd[df_bdd['fecha'] == fecha_maxima]


        #LISTAS que acumulan consecutivamente los indices y las variaciones
        lista_indices = []
        lista_var_mensual = []
        lista_var_interanual = []
        lista_var_acumulada = []

        for indice,descripcion_categoria in zip(df_categorias['id_categoria'],df_categorias['categoria_descripcion']):


            #Obtenemos el valor actual y el valor del mes anterior de la misma categoria --> SE USA EL MISMO VALOR PARA CADA VARIACION
            valor_actual = df_bdd_ultima_fecha['valor'][df_bdd_ultima_fecha['sector_productivo'] == indice].values[0]

            # === CALCULO VARIACION MENSUAL

            valor_mes_anterior = df_bdd['valor'][ (df_bdd['fecha'].dt.year == fecha_maxima.year)
                                                & (df_bdd['fecha'].dt.month == fecha_maxima.month - 1)
                                                & (df_bdd['sector_productivo'] == indice)].values[0]

            #Calculo final
            var_mensual = ((valor_actual / valor_mes_anterior) - 1) * 100

            # === CALCULO VARIACION INTERANUAL
            
            valor_año_anterior = df_bdd['valor'][ (df_bdd['fecha'].dt.year == fecha_maxima.year - 1)
                                                & (df_bdd['fecha'].dt.month == fecha_maxima.month)
                                                & (df_bdd['sector_productivo'] == indice)].values[0]

            #Calculo final
            var_intearnual = ((valor_actual / valor_año_anterior) - 1) * 100

            # === CALCULO VARIACION ACUMULADA    
            valor_diciembre_año_anterior = df_bdd['valor'][ (df_bdd['fecha'].dt.year == fecha_maxima.year - 1)
                                                & (df_bdd['fecha'].dt.month == 12)
                                                & (df_bdd['sector_productivo'] == indice)].values[0]

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

        return f"{nombre_mes_espanol} del {fecha_ultimo_registro.year}"
    
    def obtener_variacion_anualymensual(self):
        # Buscamos los datos de la tabla emae_variaciones y lo transformamos a un DataFrame
        nombre_tabla = 'emae_variaciones'
        query_select = f'SELECT * FROM {nombre_tabla} ORDER BY fecha DESC LIMIT 2' 
        df_bdd = pd.read_sql(query_select, self.conn)

        # Verificar que haya al menos dos registros en el DataFrame
        if len(df_bdd) >= 2:
            # Obtener los valores de los últimos dos registros
            ultima_fila = df_bdd.iloc[0]
            penultima_fila = df_bdd.iloc[1]

            # Acceder a los valores específicos
            fecha_ultima = ultima_fila['fecha']
            variacion_interanual_ultima = ultima_fila['variacion_interanual']
            variacion_mensual_ultima = ultima_fila['variacion_mensual']

            fecha_penultima = penultima_fila['fecha']
            variacion_interanual_penultima = penultima_fila['variacion_interanual']
            variacion_mensual_penultima = penultima_fila['variacion_mensual']

            # Puedes imprimir o retornar los valores según tus necesidades
            print(f"Última Fecha: {fecha_ultima}, Variación Interanual: {variacion_interanual_ultima}, Variación Mensual: {variacion_mensual_ultima}")
            print(f"Penúltima Fecha: {fecha_penultima}, Variación Interanual: {variacion_interanual_penultima}, Variación Mensual: {variacion_mensual_penultima}")

            # También puedes retornar los valores si quieres usarlos fuera de la función
            return (fecha_ultima, variacion_interanual_ultima, variacion_mensual_ultima), (fecha_penultima, variacion_interanual_penultima, variacion_mensual_penultima)
        else:
            print("No hay suficientes registros en la tabla.")
            return None, None