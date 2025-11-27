import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()

# Cargar variables de entorno desde el archivo .env
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DWH_SOCIO')

# Obtener la ruta de ejecución del script (directorio actual)
current_dir = os.getcwd()

# Construir la ruta del archivo CSV desde el directorio actual (sin barra inicial antes de 'files')
csv_file_path = os.path.join(current_dir, 'scrap_SISPER', 'files', 'datos_sisper.csv')  # Ruta al archivo CSV

# Parámetros de la conexión MySQL
host_mysql = host_dbb
user_mysql = user_dbb
password_mysql = pass_dbb
database_mysql = dbb_datalake
table_mysql = 'datos_sisper'

# Leer el CSV
df = pd.read_csv(csv_file_path)

# Asegurarse de que las columnas coincidan con las de la tabla MySQL
required_columns = [
    'agente_codigo', 'agente_cuil', 'agente_dni', 'cantidad_hijos', 'cargo_codigo',
    'cargo_descripcion', 'categoria_codigo', 'categoria_descripcion', 'clase_codigo',
    'clase_descripcion', 'codigo_estado_puesto', 'codigo_periodo_liquidacion', 
    'descripcion_periodo_liquidacion', 'estado_civil_codigo', 'estado_civil_descripcion',
    'fecha_ingreso', 'fecha_nacimiento', 'fecha_posesion', 'jurisdiccion_codigo', 
    'jurisdiccion_descripcion', 'localidad_codigo', 'localidad_descripcion', 
    'lugar_pago_codigo', 'lugar_pago_descripcion', 'organizacion_codigo', 
    'organizacion_descripcion', 'partida_presupuestaria_codigo', 
    'partida_presupuestaria_descripcion', 'puesto_laboral_codigo', 'salario_familiar', 
    'sexo', 'sueldo_bruto', 'sueldo_neto'
]

# Filtrar el DataFrame para que contenga solo las columnas necesarias
df = df[required_columns]

# Verificar si hay alguna columna faltante o no válida
missing_columns = set(required_columns) - set(df.columns)
if missing_columns:
    print(f"Advertencia: Las siguientes columnas faltan en el DataFrame: {missing_columns}")

# Función para guardar el DataFrame en MySQL usando SQLAlchemy
def saveToMySQL(df, host, user, password, database, table):
    # Crear conexión a MySQL con SQLAlchemy y pymysql
    engine_mysql = create_engine(f'mysql+pymysql://{user}:{password}@{host}:3306/{database}')
    
    # Guardar el DataFrame en la tabla de MySQL (añadiendo los datos)
    df.to_sql(table, engine_mysql, index=False, if_exists='append')  # 'append' para agregar datos a la tabla

# Guardar el DataFrame en MySQL
saveToMySQL(df, host_mysql, user_mysql, password_mysql, database_mysql, table_mysql)

print("Datos guardados en MySQL exitosamente.")
