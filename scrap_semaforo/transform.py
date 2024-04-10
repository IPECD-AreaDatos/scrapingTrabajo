import datetime

class Transform:
    #Objetivo: darle el formato adecuado a loss datos del semaforo para su posterior almacenado
    def transform_data(self,df_semaforo):

        #Copia de DF para no estropear datos originales
        df = df_semaforo.copy()

        #Obtencion de fechas
        df['fecha'] = self.convertir_fecha(df['fecha'])
        

        #Eliminacion del simbolo "%" del df
        for column in df.columns: 
            try:
                #Primero sacamos los %. Y lo pasamos a float para pasar a valores absolutos nuevamente.
                df[column] = df[column].str.replace('%', '').astype(float)
                df[column] = df[column] / 100

                #Con la func. lamda tomamos cad valor de la columna, y lo truncamos a 3 decimales
                decimals = 3
                df[column] = df[column].apply(lambda x: float(self.truncate_float_to_string(x, decimals)))
            except:
                pass
        
        return df

    # Función para convertir mes-año a datetime
    def convertir_fecha(self,df_fecha):


        lista_fechas_convertidas = []

        # Diccionario para mapear nombres de meses a números
        meses = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}

        for value in df_fecha:
            # Dividir la cadena en mes y año
            partes = value.split('-')
            mes_nombre = partes[0].lower()  # Obtener el nombre del mes en minúsculas
            año = int(partes[1]) + 2000      # Convertir el año a entero (sumar 2000)

            # Obtener el número de mes del diccionario
            if mes_nombre in meses:
                mes = meses[mes_nombre]
            else:
                raise ValueError("Nombre de mes inválido")

            # Crear un objeto datetime con el primer día del mes
            fecha_datetime = datetime.datetime(año, mes, 1)
            lista_fechas_convertidas.append(fecha_datetime)

        return lista_fechas_convertidas
    
    # Función para truncar un valor flotante a un número específico de decimales
    def truncate_float_to_string(self,value, decimals):

        # Convertir el valor a cadena y encontrar la posición del punto decimal
        str_value = str(value)
        decimal_index = str_value.find('.')
    
        # Truncar la cadena al número de decimales especificado
        truncated_str = str_value[:decimal_index + decimals+1]


        return truncated_str
