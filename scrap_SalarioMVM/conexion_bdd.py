import pymysql
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
import sqlalchemy
from datetime import datetime
import calendar


class conexionBaseDatos:

    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    #Conectamos a la BDD y configuramos 'conn' y 'cursor'
    def conectar_bdd(self):
            
        try:

            self.conn = pymysql.connect(
                host=self.host, user= self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor() #--cursor para usar BDD
        

        except Exception as e:

            print("ERROR:",e)


    #Cargamos los datos a la tabla 'salario_mvm' - Tambien detecta datos nuevos para ir cargandolos
    def cargar_datos(self,df):


        #Revisamos posiblidad de actualizacion
        try: 

            #Usaremos estos datos para ver que datos hay que agregar especificamente
            cantidad_fila_df,filas_df_bdd,bandera = self.revisar_tablas(df)

        except Exception as e:

            bandera = False

        if bandera:

            #tail toma las ultima n filas. En este caso (filas de df - filas de bdd) = filas a cargar
            df_aux = df.tail(cantidad_fila_df - filas_df_bdd)

            nombre_tabla = 'salario_mvm'

            #Query de insercion
            query_insercion = f"INSERT INTO {nombre_tabla} (fecha, salario_mvm_mensual, salario_mvm_diario, salario_mvm_hora) VALUES (%s, %s, %s,%s)"

            for fecha,mvm_mensual,mvm_dia,mvm_hora in zip(df_aux['indice_tiempo'],df_aux['salario_minimo_vital_movil_mensual'],df_aux['salario_minimo_vital_movil_diario'],df_aux['salario_minimo_vital_movil_hora']):
                
                #Ejecucion del query creado recientemente
                self.cursor.execute(query_insercion,(fecha,mvm_mensual,mvm_dia,mvm_hora))

            self.conn.commit()  # Hacer commit después de todas las inserciones
            self.cursor.close()
            self.conn.close()

            print("- SE HA PRODUCIDO UNA ACTUALIZACION DE DATOS")
            self.enviar_correo()
        else:

            print("- No existen datos nuevos para cargar.")


    #Revisaremos el numero de filas para revisar si hay datos nuevos - Lo haremos comprobando el df extraido de la web, y el df extraido de la bdd
    def revisar_tablas(self,df):

            self.conectar_bdd()

            #Buscamos los datos de la BDD
            nombre_tabla = 'salario_mvm'
            query_carga = f"SELECT * FROM {nombre_tabla}"
            df_bdd = pd.read_sql(query_carga,self.conn)


            #Revisamos si poseen la misma cantidad de filas
            filas_df_bdd = len(df_bdd)
            filas_df = len(df)

            if filas_df_bdd < filas_df: #Se producira la carga
                 
                return filas_df,filas_df_bdd,True
            
            else: #No se producira la carga
                return False
                
             
    def enviar_correo(self):  # Recibir los valores como argumentos


        #Obtencion de valores para informe por CORREO
        cadena_actual, salario_nominal, variacion_mensual, variacion_interanual, variacion_acumulada, fecha_ultimo_mes, fecha_mes_anterior, fecha_ultimo_mesAñoAnterior, diciembre_AñoAnterior = self.obtencion_valores()
        
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        asunto = f'Modificación en la base de datos - Salario MVM - {cadena_actual}'
        mensaje = f'''\
            <html>
            <body>
            <h2 style="font-size: 24px;"><strong> DATOS NUEVOS EN LA TABLA DE SALARIO MINIMO VITAL Y MOVIL A {cadena_actual.upper()}. </strong></h2>

            <p>* Salario Nominal de {fecha_ultimo_mes}: <span style="font-size: 17px;"><b>${salario_nominal}</b></span></p>
            <hr>
            <p>* Variacion mensual desde {fecha_mes_anterior} a {fecha_ultimo_mes}: <span style="font-size: 17px;"><b>{variacion_mensual:.2f}%%</b></span></p>
            <hr>
            <p>* Variacion Interanual de {fecha_ultimo_mes} a {fecha_ultimo_mesAñoAnterior}: <span style="font-size: 17px;"><b>{variacion_interanual:.2f}%</b></span></p>
            <hr>
            <p>* Variacion Acumulada de {diciembre_AñoAnterior} a {fecha_ultimo_mes}: <span style="font-size: 17px;"><b>{variacion_acumulada:.2f}%</b></span></p>
            <hr>
            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
                Dirección: Tucumán 1164 - Corrientes Capital<br>
                Contacto Coordinación General: 3794 284993</p>
            </body>
            </html>
            '''
        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    #OBJETIVO: Obtener los valores , salario nominal, variacion mensual, variacion interanual, variacion acumulada

    def obtencion_valores(self):

        #Abrimos conexion con la BDD
        self.conectar_bdd()

        #Traemos la tabla de SMVM y la tratamos como un dataframe
        nombre_tabla = 'salario_mvm'
        query_carga = f"SELECT * FROM {nombre_tabla}"
        df_bdd = pd.read_sql(query_carga,self.conn)

        
        # ===== Definimos salario nominal: es el dato crudo del smvym =====
        salario_nominal = df_bdd['salario_mvm_mensual'].iloc[-1]

        # ===== Definimos variacion mensual: ( Mes actual / mes anterior - 1 ) * 100 =====
        smvm_mes_actual = df_bdd['salario_mvm_mensual'].iloc[-1]
        smvm_mes_anterior = df_bdd['salario_mvm_mensual'].iloc[-2]
        variacion_mensual = (( smvm_mes_actual / (smvm_mes_anterior  ) ) - 1) * 100



        # ===== Definimos variacion interanual - Variacion del mes del año respecto al mismo mes del año pasado =====
        fecha_ultimo_mes = df_bdd['fecha'].iloc[-1]

        #Obtenemos el año, mes, y dia actuales - esto para obtener el valor del año pasado en el mismo mes
        año_actual = str(fecha_ultimo_mes.year)
        mes_actual = str(fecha_ultimo_mes.month)
        dia_actual = str(fecha_ultimo_mes.day)
        año_anterior = int(año_actual) - 1
        año_anterior = str(año_anterior)

        #cadena fecha actual 
        nombre_mes_ingles = calendar.month_name[fecha_ultimo_mes.month]
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
        nombre_mes_espanol = traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)
        cadena_actual = f"{nombre_mes_espanol} del {fecha_ultimo_mes.year}"

        #Construccion de la cadena para pasarla a formato de fecha
        cadene_fecha = año_anterior + "-" + mes_actual + "-" + dia_actual
        fecha_ultimo_mesAñoAnterior = datetime.strptime(cadene_fecha,'%Y-%m-%d').date()

        #Fila del salario del mismo mes, del año anterior
        fila_mes_AñoAnterior = df_bdd.loc[df_bdd['fecha'] == fecha_ultimo_mesAñoAnterior]
        smvm_año_anterior = fila_mes_AñoAnterior['salario_mvm_mensual'].values[0]

        #calculo final 
        variacion_interanual = ((salario_nominal / smvm_año_anterior) - 1) * 100


        # ==== VARIACION ACUMULADA: Cuanto aumento desde que empezo el año - se tiene encuenta desde diciembre del año pasado ====== #

        diciembre_AñoAnterior = datetime.strptime(año_anterior + "-" + "12-01",'%Y-%m-%d').date() #--> Fecha de DIC del año anterior

        #SMVM del año anterior
        valor_dic_AñoAnterior = df_bdd.loc[df_bdd['fecha'] == diciembre_AñoAnterior]
        smvm_dic_AñoAnterior = valor_dic_AñoAnterior['salario_mvm_mensual'].values[0]

        #calculo final
        variacion_acumulada = ((salario_nominal / smvm_dic_AñoAnterior) - 1) * 100

        #Mes anterior al actual - Usamos cadena ya creada antes - Hacemos operacio para reducir un mes a la fecha actual - No hay necesidad de pasarlo a formato de fecha
        mes_anterior = año_actual + "-" + str(int(mes_actual)-1) + "-" + dia_actual

        return cadena_actual, salario_nominal,variacion_mensual, variacion_interanual, variacion_acumulada, fecha_ultimo_mes, mes_anterior ,fecha_ultimo_mesAñoAnterior, diciembre_AñoAnterior




        

        


        




