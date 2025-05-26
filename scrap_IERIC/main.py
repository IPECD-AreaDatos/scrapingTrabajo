from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from downloadArchive import downloadArchive
from readFileActividad import readFileActividad
from readFilePuestosDeTrabajo import readFileOcupacion
from readFileIngreso import readFileSalario

# Cargar variables de entorno
load_dotenv()

host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

if __name__ == '__main__':
    print("🔽 Descargando archivos...")
    downloadArchive().descargar_archivo()

    print("📄 Leyendo archivos de actividad...")
    df_actividad = readFileActividad().read_file()


    print("📄 Leyendo archivos de ocupación...")
    df_ocupacion = readFileOcupacion().read_file()

    print("📄 Leyendo archivos de salario...")
    df_salario = readFileSalario().read_file()


    print("🔌 Conectando a la base de datos...")
    engine = create_engine(f"mysql+pymysql://{user_dbb}:{pass_dbb}@{host_dbb}/{dbb_datalake}")

    print("📤 Subiendo datos a IERIC_ACTIVIDAD...")
    df_actividad.to_sql(name="ieric_actividad", con=engine, if_exists="replace", index=False)

    print("📤 Subiendo datos a IERIC_OCUPACION...")
    df_ocupacion.to_sql(name="ieric_puestos_trabajo", con=engine, if_exists="replace", index=False)

    print("📤 Subiendo datos a IERIC_INGRESO...")
    df_salario.to_sql(name="ieric_ingreso", con=engine, if_exists="replace", index=False)

    print("✅ Proceso finalizado correctamente.")
