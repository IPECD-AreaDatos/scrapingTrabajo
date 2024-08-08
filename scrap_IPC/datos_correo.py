import mysql
import mysql.connector
from pymysql import connect
from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
import numpy as np
from armadoXLSDataNacion import LoadXLSDataNacion
from datetime import datetime
import calendar
import os
import xlrd

class Correo: 
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def enviar_correo(self):
        # datos del correo
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['samaniego18@gmail.com','benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com','alejandrobrunel@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        email_receptores =  ['manumarder@gmail.com']

        # Calculo de variaciones y fechas de los valores a publicar en el correo
        mensaje_variaciones,fecha = self.variaciones(1)
        mensaje_variaciones_nea,fecha = self.variaciones(5)

        fecha_ultimo_registro = self.obtener_mes_actual(fecha)

        asunto = f'ACTUALIZACION - INDICE DE PRECIOS AL CONSUMIDOR (IPC) - {fecha_ultimo_registro}'

        mensaje = f'''
        <html>
        <h2 style="font-size: 24px;"><strong>NUEVOS DATOS DEL INDICE DE PRECIOS AL CONSUMIDOR (IPC) DE {fecha_ultimo_registro.upper()}.</strong></h2>
        <body>
        <hr>
        '''
        
        # Agregamos las variaciones al mensaje
        variaciones_nacionales = f'''
        <h3> Variaciones a nivel Nacional - Argentina </h3>
        ''' + mensaje_variaciones + "<hr>"

        variaciones_nea = f'''
                <h3> Variaciones Nordeste(NEA) - Argentina </h3>
        ''' + mensaje_variaciones_nea + "<hr>"

        # Obtenemos los datos de variaciones para la tabla, creamos la tabla y el pie de la hoja
        datos_tabla = self.variaciones_nea()
        tabla = f''' 
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 10px;">

        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">  VAR. MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. ACUMULADA </th>
        {datos_tabla}
        </table> 
        <hr>
            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>
        </body>
        </html>
        '''
        # juntamos todo el cuerpo del correo                                                                                                                                                                                                                                      
        cadena_final = mensaje + variaciones_nacionales + variaciones_nea + tabla
        
        # enviamos el correo con cada una de sus partes
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(cadena_final, subtype = 'html')      
        contexto = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    # Calcular la variacion mensual, intearanual y acumulado del IPC de todas las regiones y categorias
    def calcular_variaciones(self):
        query = 'SELECT fecha, id_region, id_categoria, valor FROM ipc_valores'
        df = pd.read_sql(query, self.conn)
        df['fecha'] = pd.to_datetime(df['fecha'])
        
        # Se ordena el DataFrame por región, categoría y fecha, para organizar los datos cronológicamente.
        df = df.sort_values(by=['id_region', 'id_categoria', 'fecha'])
        
        # Se crea una lista vacía para almacenar los resultados históricos.
        historico = []

        # Se agrupan los datos por región e id_categoria para realizar cálculos en grupos de datos similares.
        for (region, categoria), grupo in df.groupby(['id_region', 'id_categoria']):
            
            # Se reindexa el grupo por fechas, asegurando que cada mes tenga un registro, sumando valores en caso de duplicados.
            grupo = grupo.set_index('fecha').resample('M').sum().reset_index()
            
            # Se itera sobre cada fila del grupo de datos.
            for i, row in grupo.iterrows():
                fecha = row['fecha']  # Fecha del registro actual
                valor = row['valor']  # Valor del IPC del registro actual
                
                if i == 0:
                    # Si es el primer registro del grupo, no hay datos anteriores para calcular variaciones.
                    variacion_mensual = None
                    variacion_interanual = None
                    variacion_acumulada = None
                else:
                    # Se calculan las variaciones solo si no es el primer registro.
                    
                    # Variación mensual: comparación con el valor del mes anterior.
                    valor_mes_anterior = grupo.iloc[i-1]['valor']
                    variacion_mensual = ((valor / valor_mes_anterior) - 1) * 100
                    
                    # Variación interanual: comparación con el valor del mismo mes del año anterior.
                    fecha_anio_anterior = fecha - pd.DateOffset(years=1)
                    grupo_anio_anterior = grupo[grupo['fecha'] == fecha_anio_anterior]
                    valor_anio_anterior = grupo_anio_anterior['valor'].values[0] if not grupo_anio_anterior.empty else None
                    variacion_interanual = ((valor / valor_anio_anterior) - 1) * 100 if valor_anio_anterior is not None else None
                    
                    # Variación acumulada: comparación con el valor de diciembre del año anterior.
                    grupo_diciembre_anio_anterior = grupo[(grupo['fecha'].dt.year == fecha.year - 1) & (grupo['fecha'].dt.month == 12)]
                    valor_diciembre_anio_anterior = grupo_diciembre_anio_anterior['valor'].values[0] if not grupo_diciembre_anio_anterior.empty else None
                    variacion_acumulada = ((valor / valor_diciembre_anio_anterior) - 1) * 100 if valor_diciembre_anio_anterior is not None else None
                
                # Se agrega el resultado de las variaciones a la lista histórica.
                historico.append({
                    'fecha': fecha,
                    'id_region': region,
                    'id_categoria': categoria,
                    'var_mensual': np.floor(variacion_mensual * 10) / 10 if variacion_mensual is not None else None,
                    'var_interanual': np.floor(variacion_interanual * 10) / 10 if variacion_interanual is not None else None,
                    'var_acumulada': np.floor(variacion_acumulada * 10) / 10 if variacion_acumulada is not None else None
                })
        
        # Se convierte la lista de resultados históricos en un DataFrame.
        df_resultado = pd.DataFrame(historico)
        
        # Se reemplazan los valores NaN por None para asegurar la compatibilidad con bases de datos.
        df_resultado = df_resultado.applymap(lambda x: None if pd.isna(x) else x)
        
        # Si el id_region es un tuple, se extrae el primer elemento; esto podría ocurrir por la forma en que se manejan los datos en pandas.
        df_resultado['id_region'] = df_resultado['id_region'].apply(lambda x: x[0] if isinstance(x, tuple) else x)

        return df_resultado
