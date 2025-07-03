from .extract import extract_sipa_data
from .transform import transform_sipa_data
from .load import load_sipa_data
from .send_mail import MailSipa
from etl_modular.utils.logger import setup_logger

import os
from dotenv import load_dotenv

def run_sipa():
    setup_logger("sipa")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    ruta = extract_sipa_data()
    df = transform_sipa_data(ruta)
    datos_nuevos = load_sipa_data(df)
    if datos_nuevos:
        print("✅ Datos cargados. Procediendo a enviar el correo...")
        mailer = MailSipa(
            host,
            user,
            password,
            database
        )
        mailer.send_mail()
    else:
        print("⚠️ No se detectaron datos nuevos. No se enviará correo.")
