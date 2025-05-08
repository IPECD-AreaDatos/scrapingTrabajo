from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
dbname = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

# Crear conexión con SQLAlchemy
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{dbname}")

# Subida de DataFrames
def subir_dataframe(df, tabla):
    try:
        df.to_sql(name=tabla, con=engine, if_exists='append', index=False)
        print(f"✅ Cargado correctamente en: {tabla}")
    except Exception as e:
        print(f"❌ Error al cargar en {tabla}: {e}")
