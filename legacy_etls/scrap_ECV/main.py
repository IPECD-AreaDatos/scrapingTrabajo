import os
from dotenv import load_dotenv
from connect_db import connect_db

# Importaciones de lectura
from readSheetsTasas import readSheetsTasas
from readSheetsTrabajo import readSheetsTrabajo
from readSheetsTrabajoQuintiles import readSheetsTrabajoQuintiles
from readSheetsSaludCobertura import readSheetsSaludCobertura
from readSheetsSaludConsultaEstablecimiento import readSheetsSaludConsultaEstablecimiento
from readSheetsSaludQuintilConsulta import readSheetsSaludQuintilConsulta
from readSheetsSaludQuintilCoberturaEst import readSheetsSaludQuintilCoberturaEst
from readSheetsEducacion import readSheetsEducacion
from readSheetsEducacionMay25 import readSheetsEducacionMay25
from readSheetsEducacionQuintiles import readSheetsEducacionQuintiles
from readSheetsTransporteMedios import readSheetsTransporteMedios
from readSheetsTransporteDesplazamiento import readSheetsTransporteDesplazamiento
from readSheetsTransporteTiempoMedio import readSheetsTransporteTiempoMedio
from readSheetsTransporteUniversitarios import readSheetsTransporteUniversitarios
from readSheetsPobrezaImpacto import readSheetsPobrezaImpacto
from readSheetsPlanesImpacto import readSheetsPlanesImpacto
from readSheetsQPlanes import readSheetsQPlanes

# Cargar .env
load_dotenv()
host = os.getenv('HOST_DBB')
user = os.getenv('USER_DBB')
password = os.getenv('PASSWORD_DBB')
database = os.getenv('NAME_DBB_DWH_SOCIO')

# Crear instancia de conexión
db = connect_db(host, user, password, database)

# Diccionario con funciones
opciones = {
    "tasas": lambda: db.connect_db_tasas(readSheetsTasas().leer_datos_tasas()),
    "trabajo": lambda: db.connect_db_trabajo(readSheetsTrabajo().leer_datos_trabajo()),
    "trabajo_quintiles": lambda: db.connect_db_trabajo_quintiles(readSheetsTrabajoQuintiles().leer_datos_trabajo_quintiles()),
    "salud_cobertura": lambda: db.connect_db_salud_cobertura(readSheetsSaludCobertura().leer_datos_salud_cobertura()),
    "salud_consulta_establecimiento": lambda: db.connect_db_salud_consulta_establecimiento(readSheetsSaludConsultaEstablecimiento().leer_datos_salud_consulta_establecimiento()),
    "salud_quintil_consulta": lambda: db.connect_db_salud_quintil_consulta(readSheetsSaludQuintilConsulta().leer_datos_salud_quintil_consulta()),
    "salud_quintil_cobertura_est": lambda: db.connect_db_salud_quintil_cobertura_est(readSheetsSaludQuintilCoberturaEst().leer_datos_salud_quintil_cobertura_est()),
    "educacion": lambda: db.connect_db_educacion(readSheetsEducacion().leer_datos_educacion()),
    "educacion_may25": lambda: db.connect_db_educacion_may_25(readSheetsEducacionMay25().leer_datos_educacionMay25()),
    "educacion_quintiles": lambda: db.connect_db_educacion_quintiles(readSheetsEducacionQuintiles().leer_datos_educacionQuintiles()),
    "transporte_medios": lambda: db.connect_db_transporte_medios(readSheetsTransporteMedios().leer_datos_trabajo()),
    "transporte_desplazamiento": lambda: db.connect_db_transporte_desplazamiento(readSheetsTransporteDesplazamiento().leer_datos_trabajo()),
    "transporte_tiempo_medio": lambda: db.connect_db_transporte_tiempo_medio(readSheetsTransporteTiempoMedio().leer_datos_trabajo()),
    "transporte_universitarios": lambda: db.connect_db_transporte_universitarios(readSheetsTransporteUniversitarios().leer_datos_trabajo()),
    "pobreza_impacto": lambda: db.connect_db_pobreza_impacto(readSheetsPobrezaImpacto().leer_datos_pobreza_impacto()),
    "planes_impacto": lambda: db.connect_db_planes_impacto(readSheetsPlanesImpacto().leer_datos_planes_impacto()),
    "q_planes": lambda: db.connect_db_q_planes(readSheetsQPlanes().leer_datos_q_planes()),
}

def mostrar_menu():
    print("\n📊 Menú de carga de datos")
    print("-" * 40)
    for i, clave in enumerate(opciones.keys(), start=1):
        print(f"{i}. {clave}")
    print(f"{len(opciones)+1}. Ejecutar TODO")
    print("0. Salir")
    print("-" * 40)

def ejecutar_menu():
    while True:
        mostrar_menu()
        eleccion = input("👉 Seleccioná una opción: ").strip()

        if eleccion == "0":
            print("👋 Salida del programa.")
            break
        elif eleccion == str(len(opciones)+1):
            print("\n▶️ Ejecutando todos los módulos...")
            for nombre, funcion in opciones.items():
                print(f"\n📁 Ejecutando: {nombre}")
                funcion()
        elif eleccion.isdigit() and 1 <= int(eleccion) <= len(opciones):
            clave = list(opciones.keys())[int(eleccion)-1]
            print(f"\n▶️ Ejecutando módulo: {clave}")
            opciones[clave]()
        else:
            print("❌ Opción inválida. Intentá de nuevo.")

if __name__ == "__main__":
    ejecutar_menu()
