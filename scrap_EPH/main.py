import os
from readSheetsTrabajo import readSheetsTrabajoEPH
from readSheetsPobreza import readSheetsPobrezaEPH
from connect_db import connect_db
from dotenv import load_dotenv

# ✅ Cargar variables de entorno
load_dotenv()
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_dwh = os.getenv('NAME_DBB_DWH_SOCIO')

if __name__ == "__main__":
    db = connect_db(host_dbb, user_dbb, pass_dbb, dbb_dwh)

    # ✅ Hoja "Trabajo_EPH"
    df_trabajo_eph = readSheetsTrabajoEPH().leer_datos_tasas()
    db.actualizar_eph_trabajo_tasas(df_trabajo_eph)

    # ✅ Hoja "Pobreza_EPH"
    df_pobreza_eph = readSheetsPobrezaEPH().leer_datos_tasas()
    db.actualizar_eph_pobreza(df_pobreza_eph)
