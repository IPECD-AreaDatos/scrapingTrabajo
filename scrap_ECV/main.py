import os
import sys
from readSheetsTasas import readSheetsTasas
from connect_db import connect_db
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

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('dwh_sociodemografico')


if __name__ ==  "__main__":
    print("Las credenciales son", credenciales.host,credenciales.user,credenciales.password,credenciales.database)
    df_tasas = readSheetsTasas().leer_datos_tasas()
    connect_db().connect_db_tasas(df_tasas, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_trabajo = readSheetsTrabajo().leer_datos_trabajo()
    connect_db().connect_db_trabajo(df_trabajo, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_trabajo_quintiles = readSheetsTrabajoQuintiles().leer_datos_trabajo_quintiles()
    connect_db().connect_db_trabajo_quintiles(df_trabajo_quintiles, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_salud_cobertura = readSheetsSaludCobertura().leer_datos_salud_cobertura()
    connect_db().connect_db_salud_cobertura(df_salud_cobertura, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_salud_consulta_establecimiento = readSheetsSaludConsultaEstablecimiento().leer_datos_salud_consulta_establecimiento()
    connect_db().connect_db_salud_consulta_establecimiento(df_salud_consulta_establecimiento, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_salud_quintil_consulta= readSheetsSaludQuintilConsulta().leer_datos_salud_quintil_consulta()
    connect_db().connect_db_salud_quintil_consulta(df_salud_quintil_consulta, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_salud_quintil_cob_est= readSheetsSaludQuintilCoberturaEst().leer_datos_salud_quintil_cobertura_est()
    connect_db().connect_db_salud_quintil_cobertura_est(df_salud_quintil_cob_est, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_educacion = readSheetsEducacion().leer_datos_educacion()
    connect_db().connect_db_educacion(df_educacion, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_educacion_may_25 = readSheetsEducacionMay25().leer_datos_educacionMay25()
    connect_db().connect_db_educacion_may_25(df_educacion_may_25, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_educacion_quintiles = readSheetsEducacionQuintiles().leer_datos_educacionQuintiles()
    connect_db().connect_db_educacion_quintiles(df_educacion_quintiles, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_transporte_medios = readSheetsTransporteMedios().leer_datos_trabajo()
    connect_db().connect_db_transporte_medios(df_transporte_medios, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_transporte_desplazamiento = readSheetsTransporteDesplazamiento().leer_datos_trabajo()
    connect_db().connect_db_transporte_desplazamiento(df_transporte_desplazamiento, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_transporte_tiempo_medio = readSheetsTransporteTiempoMedio().leer_datos_trabajo()
    connect_db().connect_db_transporte_tiempo_medio(df_transporte_tiempo_medio, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_transporte_universitarios = readSheetsTransporteUniversitarios().leer_datos_trabajo()
    connect_db().connect_db_transporte_universitarios(df_transporte_universitarios, credenciales.host, credenciales.user, credenciales.password, credenciales.database)