import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import calendar
from email.message import EmailMessage
import ssl
import smtplib
import os

class DatabaseManager:
    def __init__(self, host, user, password, database):
        """
        Inicializa la conexión a la base de datos con las credenciales proporcionadas.
        """
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def __del__(self):
        """
        Asegura que el cursor y la conexión se cierren cuando la instancia se destruye.
        """
        self.cursor.close()
        self.connection.close()

    def update_database_with_new_data(self, df):
        """
        Actualiza la base de datos con nuevos datos de un DataFrame de Pandas. Solo inserta los datos
        si el número de filas en la base de datos difiere del número de filas en el DataFrame.

        Args:
        df (pd.DataFrame): DataFrame con los nuevos datos a insertar.
        """
        table_name = 'ipicorr'
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        filas_BD = self.cursor.fetchone()[0]

        print(f"Base de datos: {filas_BD}, DataFrame: {len(df)}")
        ruta_archivo_grafico = self.generar_y_guardar_grafico(df)

        if filas_BD != len(df):
            df_datos_nuevos = df.tail(len(df) - filas_BD)
            print("Nuevos datos para insertar:", df_datos_nuevos)

            # Preparar y ejecutar las sentencias INSERT de manera eficiente
            insert_query = f"INSERT INTO {table_name} (Fecha, var_interanual_ipicorr, var_interanual_alimentos, var_interanual_textil, var_interanual_maderas, var_interanual_minnoMetalicos, var_interanual_metales) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            
            # Lista para almacenar los datos a insertar
            data_to_insert = []

            for index, row in df_datos_nuevos.iterrows():
                data_to_insert.append((
                    row['Fecha'],
                    self.format_value(row['Var_Interanual_IPICORR']),
                    self.format_value(row['Var_Interanual_Alimentos']),
                    self.format_value(row['Var_Interanual_Textil']),
                    self.format_value(row['Var_Interanual_Maderas']),
                    self.format_value(row['Var_Interanual_MinNoMetalicos']),
                    self.format_value(row['Var_Interanual_Metales'])
                ))

            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()

            print(f"{len(data_to_insert)} nuevos registros insertados.")
            df_datos_nuevos['Fecha'] = pd.to_datetime(df_datos_nuevos['Fecha'], format='%Y-%m-%d')
            exit()
            self.envio_correo(df_datos_nuevos, ruta_archivo_grafico)
        else:
            print("No se encontraron nuevos datos para insertar.")

    def format_value(self, value):
        """
        Formatea el valor numérico eliminando comas, signos de porcentaje y convirtiéndolo a un flotante.

        Args:
        value (str): Valor a formatear.

        Returns:
        float: Valor formateado.
        """
        return (float(value.replace(',', '').replace('%', '')) / 10) / 100
    

    def envio_correo(self, df_datos_nuevos, ruta_archivo_grafico): 
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']
        fecha = df_datos_nuevos["Fecha"].iloc[-1]  # Accede a la última fecha desde el DataFrame
        fecha_arreglada =self.obtener_ultimafecha_actual(fecha)
        asunto = f'ACTUALIZACION - IPICORR - {fecha_arreglada}'
        mensaje = f'''\
            <html>
            <body>
            <h2>Se ha producido una modificación en la base de datos. La tabla de IPICORR contiene nuevos datos.</h2>
            <p>*Variacion Interanual IPICORR: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_IPICORR"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Alimentos: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Alimentos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Textil: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Textil"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Maderas: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Maderas"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual min. No Metalicos: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_MinNoMetalicos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Metales: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Metales"].iloc[-1]}</b></span></p>
            <hr>
            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>
            </body>
            </html>
                    '''
        em.set_content(mensaje, subtype='html')

        # Adjuntar el gráfico como imagen incrustada
        with open(ruta_archivo_grafico, 'rb') as archivo:
            contenido_archivo = archivo.read()
            em.add_related(contenido_archivo, 'image', 'png', cid='grafico_ipicorr')
                
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
                
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    def obtener_ultimafecha_actual(self,fecha_ultimo_registro):
        
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
    
    def generar_y_guardar_grafico(self, df, columna_fecha='Fecha', columna_valor='Var_Interanual_IPICORR', nombre_archivo='variacion_interanual_ipicorr.png'):
        """
        Genera un gráfico de línea a partir de un DataFrame y guarda el resultado en un archivo PNG dentro de la carpeta 'files'.

        Args:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        columna_fecha (str): Nombre de la columna en df que contiene las fechas.
        columna_valor (str): Nombre de la columna en df que contiene los valores a graficar.
        nombre_archivo (str): Nombre del archivo donde se guardará el gráfico.
        """
        # Preparación de los datos
        df[columna_fecha] = pd.to_datetime(df[columna_fecha])
        df[columna_valor] = df[columna_valor].str.replace(',', '.').str.replace('%', '').astype(float)/100
    
        # Generación del gráfico
        plt.figure(figsize=(12, 7))
        # Asegúrate de multiplicar df[columna_valor] por 100 aquí para que los valores se muestren como porcentajes
        plt.plot(df[columna_fecha], df[columna_valor] * 100, '-o', color='green')  # Multiplicación por 100

        # Iterar sobre el DataFrame para poner los valores en los puntos
        for i, punto in df.iterrows():
            # Asegúrate de que el texto también refleje el valor como porcentaje, multiplicando por 100
            plt.text(punto[columna_fecha], punto[columna_valor] * 100, f'{punto[columna_valor]:.2%}', color='black', ha='left', va='bottom')

        plt.title('Variación Interanual IPICORR')
        plt.xlabel('Fecha')
        plt.ylabel('Variación (%)')
        plt.grid(True)

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Asegurarse de que la carpeta "files" exista
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)

        # Construir la ruta completa del archivo a guardar
        nombre_archivo_completo = os.path.join(carpeta_guardado, nombre_archivo)

        # Guardar el gráfico
        plt.savefig(nombre_archivo_completo)
        plt.close()
        
        # Devuelve la ruta completa del archivo para su uso posterior
        return nombre_archivo_completo