import os
import pandas as pd
from dotenv import load_dotenv

from extractSheets import HomePage
from readDataExcel import readDataExcel
from conexionBaseDatos import conexionBaseDatos
from send_mail_sipa import MailSipa

# === Cargar variables de entorno
load_dotenv()
host = os.getenv('HOST_DBB')
user = os.getenv('USER_DBB')
password = os.getenv('PASSWORD_DBB')
database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

# === Ruta del archivo Excel
directorio = os.path.dirname(os.path.abspath(__file__))
ruta_archivo = os.path.join(directorio, 'files', 'SIPA.xlsx')

"""
TENER CUIDADO EN LA CARGA CON EL NOMBRE DE CIUDAD AUTONOMA DE BUENOS AIRES Y RIO NEGRO
Solucion: 
Copiar el nombre de la provincia de la hoja de con estacionalidad
Y pegarlo en el nombre de la provincia de la hoja sin estacionalidad
"""

def main():
    # 1. Descargar archivo actualizado desde la web
    HomePage()

    # 2. Leer y construir el DataFrame completo
    extractor = readDataExcel()
    df = extractor.get_dataframe(ruta_archivo)

    # 3. Cargar todo el pipeline de SIPA
    conexion = conexionBaseDatos(host, user, password, database)
    if conexion.load_all(df):  # ← método único que hace todo
        print("✅ Datos cargados y procesados correctamente")

        # 4. Enviar informe por correo
        mailer = MailSipa(host, user, password, database)
        mailer.connect_db()
        mailer.send_mail()
        print("📧 Correo enviado exitosamente")
    else:
        print("⚠️ No se detectaron datos nuevos. No se envió el correo.")

if __name__ == '__main__':
    main()
