import mysql.connector
import pandas as pd
from datetime import datetime
import calendar
from email.message import EmailMessage
import ssl
import smtplib

class connect_db:
    def connect_db_tasas(self, df, host, user, password, database):
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_tasas'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (fecha, trimestre, aglomerado, tasa_de_empleo, tasa_de_desocupacion, tasa_de_actividad, tasa_de_inactividad) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Fecha'], row['Trimestre'], row['Aglomerado'], row['Tasa de empleo'], row['Tasa de desocupación'], row['Tasa de actividad'], row['Tasa de inactividad']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")
        else: 
            print("Se verifico la tabla de IPICORR")

        print("Se verifico Tabla Tasas")
            
        cursor.close()
        conn.close()



    def connect_db_trabajo(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_trabajo'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:
            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")
            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, trimestre, tasa_de_actividad, tasa_de_empleo, tasa_de_desocupación, trabajo_privado, trabajo_público, trabajo_otro, trabajo_privado_registrado, trabajo_privado_no_registrado, salario_promedio_público, salario_promedio_privado, salario_promedio_privado_registrado, salario_promedio_privado_no_registrado, patron, cuenta_propia, empleado_obrero, trabajador_familiar_sin_remuneración) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Trimestre'], row['Tasa de Actividad'], row['Tasa de Empleo'], row['Tasa de desocupación'], row['Trabajo Privado'], row['Trabajo Público'], row['Trabajo Otro'], row['Trabajo Privado Registrado'], row['Trabajo Privado No Registrado'], row['Salario Promedio Público'], row['Salario Promedio Privado'], row['Salario Promedio Privado Registrado'], row['Salario Promedio Privado No Registrado'], row['Patron'], row['Cuenta Propia'], row['Empleado/Obrero'], row['Trabajador familiar sin remuneración']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")
        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Trabajo")
        cursor.close()
        conn.close()

    def connect_db_trabajo_quintiles(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_trabajo_quintiles'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:
            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")
            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, trimestre, quintil, empleo_público, empleo_privado, empleo_otro, patron, cuenta_propia, obrero_o_empleado, trabajador_familiar_sin_remuneracion, primaria_incompleta, primaria_completa, secundaria_incompleta, secundaria_completa, superior_o_universitario_incompleto, superior_o_universitario_completo, sin_instruccion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Trimestre'], row['Quintil'], 
                row['Empleo Público'], row['Empleo Privado'], row['Empleo Otro'], 
                row['Patron'], row['Cuenta Propia'], row['Obrero o Empleado'], 
                row['Trabajador Familiar sin Remuneracion'], row['Primaria Incompleta'], 
                row['Primaria Completa'], row['Secundaria Incompleta'], 
                row['Secundaria Completa'], row['Superior o Universitario Incompleto'], 
                row['Superior o Universitario Completo'], row['Sin Instruccion']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")
        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Trabajo Quintiles")

        cursor.close()
        conn.close()

    def connect_db_salud_cobertura(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_salud_cobertura'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:
            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")
            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, semestre, cobertura, planes_y_seguros, no_paga_ni_le_descuentan) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Semestre'], row['Cobertura'], 
                row['Planes y seguros'], row['No paga ni le descuentan']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")
        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Salud Cobertura")

        cursor.close()
        conn.close()

    def connect_db_salud_consulta_establecimiento(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_salud_consulta_establecimiento'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")
            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, semestre, cobertura, si_consulto, no_consulto, dolencia_afeccion_enfermedad, control_prevencion, establecimiento_privado, establecimiento_publico) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Semestre'], row['Cobertura'], 
                                row['Si consulto'], row['No consulto'], row['Dolencia/ afección/ enfermedad'], row['Control/ prevención'], row['Establecimiento_Privado'], row['Establecimiento_Publico']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Salud Consulta Establecimiento")

        cursor.close()
        conn.close()

    def connect_db_salud_quintil_consulta(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_salud_quintil_consulta'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, semestre, cobertura, quintil, si_consulto, no_consulto) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Semestre'], row['Cobertura'], row['Quintil'],
                                row['Si consulto'], row['No consulto']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Salud Quintil Consulta")

        cursor.close()
        conn.close()

    def connect_db_salud_quintil_cobertura_est(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_salud_quintil_cobertura_est'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, semestre, quintil, obra_social_prepaga, planes_y_seguros, sin_cobertura, establecimiento_privado, establecimiento_publico) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Semestre'], row['Quintil'],
                                row['Obra social/Prepaga'], row['Planes y seguros'], row['Sin Cobertura'], row['Establecimiento_Privado'], row['Establecimiento_Publico']))
            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Salud Quintil Cobertura Establecimeinto")

        cursor.close()
        conn.close()

    def connect_db_educacion(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_educacion'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, trimestre, fecha, nivel_educativo, asiste, no_asiste_pero_asistio, nunca_asistio, institucion_publica, institucion_privada, edad_promedio_abandono, sobreedad, acceso_a_internet_fijo, calidad_de_vivienda_suficiente, calidad_de_vivienda_parcialmente_insuficiente, calidad_de_vivienda_insuficiente, vivienda_cercana_a_un_basural, vivienda_en_villa_emergencia, automovil, motocicleta, bicicleta, caminata, taxi_remis, transporte_urbano, otros) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Trimestre'], row['Fecha'], row['Nivel Educativo'], row['Asiste'], row['No asiste pero asistió'], row['Nunca asistió'], row['Institución Pública'], row['Institución Privada'], row['Edad promedio abandono'], row['Sobreedad'], row['Acceso a internet fijo'], row['Calidad de vivienda suficiente'], row['Calidad de vivienda parcialmente insuficiente'], row['Calidad de vivienda insuficiente'], row['Vivienda cercana a un basural'], row['Vivienda en villa emergencia'], row['Automóvil'], row['Motocicleta'], row['Bicicleta'], row['Caminata'], row['Taxi/Remis'], row['Transporte Urbano'], row['Otros']))

            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")

        print("Se verifico Tabla Educacion")

        cursor.close()
        conn.close()

    def connect_db_educacion_may_25(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_educacion_may25'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, trimestre, fecha, primaria_incompleta, primaria_completa, secundaria_incompleta, secundaria_completa, superior_incompleto, superior_completo, sin_instruccion, eph_universitario, eph_secundaria) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Trimestre'], row['Fecha'], row['Primaria incompleta'], row['Primaria completa'], row['Secundaria incompleta'], row['Secundaria completa'], row['Superior incompleto'], row['Superior completo'], row['Sin instrucción'], row['EPH Universitario'], row['EPH Secundaria']))

            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")
    
        print("Se verifico Tabla Educacion May 25")

        cursor.close()
        conn.close()

    def connect_db_educacion_quintiles(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_educacion_quintiles'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, trimestre, quintil, primaria_incompleta, primaria_completa, secundaria_incompleta, secundaria_completa, superior_incompleto, superior_completo, sin_instruccion, asistencia_escolar, institucion_publica, institucion_privada) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['Aglomerado'], row['Año'], row['Fecha'], row['Trimestre'], row['Quintil'], row['Primaria incompleta'], row['Primaria completa'], row['Secundaria incompleta'], row['Secundaria completa'], row['Superior incompleto'], row['Superior completo'], row['Sin instrucción'], row['Asisencia escolar'], row['Institución Pública'], row['Institución Privada']))


            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else: 
            print("Se verifico la tabla de IPICORR")
            

        print("Se verifico Tabla Educacion Quintiles")

        cursor.close()
        conn.close()

    def connect_db_transporte_medios(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name = 'ecv_transporte_medios'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        longitud_df = len(df)

        if filas_BD != longitud_df:
            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():
                # Convertir la fecha al formato adecuado para MySQL
                row['fecha'] = pd.to_datetime(row['fecha'], errors='coerce')
                if pd.isna(row['fecha']):
                    row['fecha'] = None
                else:
                    row['fecha'] = row['fecha'].strftime('%Y-%m-%d')

                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, trimestre, fecha, automovil, motocicleta, bicicleta, caminata, taxi_o_remis, transporte_publico, otros) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)

                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['aglomerado'], row['año'], row['trimestre'], row['fecha'], row['automovil'], row['motocicleta'], row['bicicleta'], row['caminata'], row['taxi_o_remis'], row['transporte_publico'], row['otros']))

            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")

        else:
            print("Se verifico la tabla de ecv")

        print("Se verifico Tabla Transporte Medios")

        cursor.close()
        conn.close()

    def connect_db_transporte_desplazamiento(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_transporte_desplazamiento'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():

                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, trimestre,fecha, no_desplaza, desplaza_en_municipio, desplaza_fuera_municipio, sin_desplazamiento_fijo, tarda_menos_de_15_minutos, tarda_entre_15_y_30_minutos, tarda_entre_30_min_y_1_hora, tarda_mas_de_1_hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['aglomerado'], row['año'], row['trimestre'], row['fecha'], row['no_desplaza'], row['desplaza_en_municipio'], row['desplaza_fuera_municipio'], row['sin_desplazamiento_fijo'], row['tarda_menos_de_15_minutos'], row['tarda_entre_15_y_30_minutos'], row['tarda_entre_30_min_y_1_hora'], row['tarda_mas_de_1_hora']))


            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")


        else: 
            print("Se verifico la tabla de ecv")
            

        print("Se verifico Tabla Transporte Desplazamientos")

        cursor.close()
        conn.close()
        
    def connect_db_transporte_tiempo_medio(self, df, host, user, password, database): 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_transporte_tiempo_medio'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():

                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, fecha, trimestre, transporte, tarda_menos_de_15_minutos, tarda_entre_15_y_30_minutos, tarda_entre_30_min_y_1_hora, tarda_mas_de_1_hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['aglomerado'], row['año'], row['fecha'], row['trimestre'], row['transporte'], row['tarda_menos_de_15_minutos'], row['tarda_entre_15_y_30_minutos'], row['tarda_entre_30_min_y_1_hora'], row['tarda_mas_de_1_hora']))


            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")


        else: 
            print("Se verifico la tabla de ecv")
            

        print("Se verifico Tabla Transporte Desplazamientos")

        cursor.close()
        conn.close()
        
    def connect_db_transporte_universitarios(self, df, host, user, password, database):
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'ecv_transporte_universitarios'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base de datos:", filas_BD)
        print("DataFrame:", len(df))
        longitud_df = len(df)

        if filas_BD != longitud_df:

            query_truncate = f'TRUNCATE TABLE {table_name}'
            cursor.execute(query_truncate)
            conn.commit()

            print(f"Tabla {table_name} truncada.")

            for index, row in df.iterrows():

                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (aglomerado, año, trimestre, fecha, automovil, motocicleta, bicicleta, caminata, taxi_o_remis, transporte_urbano, transporte_interurbano, otros) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # Rellenar los valores faltantes (NaN) con None
                row = row.where(pd.notnull(row), None)
                
                # Ejecutar la sentencia SQL de inserción
                cursor.execute(sql_insert, (row['aglomerado'], row['año'], row['trimestre'], row['fecha'],  row['automovil'], row['motocicleta'], row['bicicleta'], row['caminata'], row['taxi_o_remis'], row['transporte_urbano'], row['transporte_interurbano'], row['otros']))


            conn.commit()
            print(f"Se han insertado {longitud_df} nuevos registros en la tabla {table_name}.")


        else: 
            print("Se verifico la tabla de ecv")
            

        print("Se verifico Tabla Transporte Desplazamientos")

        cursor.close()
        conn.close()
        