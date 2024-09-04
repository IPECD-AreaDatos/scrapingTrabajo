import pandas as pd 
from bs4 import BeautifulSoup

class ReadDataHTM:
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
            #df = df.iloc[17:].reset_index(drop=True)

            

            # Añadir el DataFrame a la lista
            df_list.append(df)

        # Concatenar todos los DataFrames en uno solo
        combined_df = pd.concat(df_list, ignore_index=True)


        return combined_df

# Ruta al archivo HTM
combined_df = ReadDataHTM().leer_datos('C:\\Users\\manum\\OneDrive\\Escritorio\\scrapingTrabajo\\scrap_canasta_basica.py\\AZUCAR.HTM')

# Mostrar el DataFrame combinado
print("DataFrame combinado:")
print(combined_df.head(50))
print(combined_df.iloc[:, :1].head(50))
