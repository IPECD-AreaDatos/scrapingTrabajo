import pandas as pd 
from bs4 import BeautifulSoup

class ReadDataHTM:
    def dividir_cadena(self, cadena, posiciones):
        valores = []
        inicio = 0
        for pos in posiciones:
            if pos == -1:  # Si es -1, toma todo el resto de la cadena
                valores.append(cadena[inicio:])
                break
            else:
                valores.append(cadena[inicio:inicio+pos])
                inicio += pos
        return valores
    
    def leer_datos(self, htm_path):
        # Leer el archivo HTM y cargar el contenido
        with open(htm_path, 'r', encoding='ISO-8859-1') as file:
            soup = BeautifulSoup(file, 'html.parser')

        # Encontrar todas las tablas en el archivo HTML
        tables = soup.find_all('table')
        print(f"Se encontraron {len(tables)} tablas en el archivo HTML.")

        # Lista para almacenar los DataFrames de las tablas
        df_list = []

        # Procesar cada tabla encontrada
        for table in tables:
            rows = table.find_all('tr')  # Encontrar todas las filas en la tabla
            table_data = []

            for row in rows:
                cols = row.find_all(['td', 'th'])  # Encontrar todas las celdas en la fila
                row_data = [col.get_text(strip=True) for col in cols]  # Obtener el texto de cada celda
                table_data.append(row_data)  # Agregar la fila a la lista de datos de la tabla

            # Crear un DataFrame de Pandas para la tabla actual
            df = pd.DataFrame(table_data)
            df = df.iloc[17:].reset_index(drop=True)
            


            

            # Añadir el DataFrame a la lista
            df_list.append(df)

        # Coincatenar todos los DataFrames en uno solo
        combined_df = pd.concat(df_list, ignore_index=True)

        columnas=['inf.','for','pan','vis','raz','raz for','obs','precio','p_normal','ant_precio','ant_p_normal','var','t','ta','atributos']
        divisones = [7,1,2,2,1,1,2,3,5,5,5,2,1,1,-1]  # -1 para tomar el resto de la cadena

        # Aplicar la función a la columna de cadenas para crear un nuevo DataFrame
        df_separado = combined_df[0].apply(lambda x: pd.Series(self.dividir_cadena(x, divisones)))
                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
        # Asignar los nombres de las columnas
        df_separado.columns = columnas
        return df_separado
    
        

# Ruta al archivo HTM
#combined_df = ReadDataHTM().leer_datos('C:\\Users\\manum\\OneDrive\\Escritorio\\scrapingTrabajo\\scrap_canasta_basica.py\\AZUCAR.HTM')
combined_df = ReadDataHTM().leer_datos('C://Users//Usuario//Desktop//scrapingTrabajo//scrap_canasta_basica.py//AZUCAR.HTM')
 
# Mostrar el DataFrame combinado
print("DataFrame combinado:")
print(combined_df.head(50))
