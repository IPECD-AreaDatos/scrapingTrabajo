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
# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_SOCIO'))

if __name__ ==  "__main__":
    df_tasas = readSheetsTasas().leer_datos_tasas()
    connect_db().connect_db_tasas(df_tasas, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_trabajo = readSheetsTrabajo().leer_datos_trabajo()
    connect_db().connect_db_trabajo(df_trabajo, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_trabajo_quintiles = readSheetsTrabajoQuintiles().leer_datos_trabajo_quintiles()
    connect_db().connect_db_trabajo_quintiles(df_trabajo_quintiles, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_salud_cobertura = readSheetsSaludCobertura().leer_datos_salud_cobertura()
    connect_db().connect_db_salud_cobertura(df_salud_cobertura, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_salud_consulta_establecimiento = readSheetsSaludConsultaEstablecimiento().leer_datos_salud_consulta_establecimiento()
    connect_db().connect_db_salud_consulta_establecimiento(df_salud_consulta_establecimiento, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_salud_quintil_consulta= readSheetsSaludQuintilConsulta().leer_datos_salud_quintil_consulta()
    connect_db().connect_db_salud_quintil_consulta(df_salud_quintil_consulta, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_salud_quintil_cob_est= readSheetsSaludQuintilCoberturaEst().leer_datos_salud_quintil_cobertura_est()
    connect_db().connect_db_salud_quintil_cobertura_est(df_salud_quintil_cob_est, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_educacion = readSheetsEducacion().leer_datos_educacion()
    connect_db().connect_db_educacion(df_educacion, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_educacion_may_25 = readSheetsEducacionMay25().leer_datos_educacionMay25()
    connect_db().connect_db_educacion_may_25(df_educacion_may_25, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_educacion_quintiles = readSheetsEducacionQuintiles().leer_datos_educacionQuintiles()
    connect_db().connect_db_educacion_quintiles(df_educacion_quintiles, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_transporte_medios = readSheetsTransporteMedios().leer_datos_trabajo()
    connect_db().connect_db_transporte_medios(df_transporte_medios, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_transporte_desplazamiento = readSheetsTransporteDesplazamiento().leer_datos_trabajo()
    connect_db().connect_db_transporte_desplazamiento(df_transporte_desplazamiento, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_transporte_tiempo_medio = readSheetsTransporteTiempoMedio().leer_datos_trabajo()
    connect_db().connect_db_transporte_tiempo_medio(df_transporte_tiempo_medio, host_dbb,user_dbb,pass_dbb,dbb_dwh)
    df_transporte_universitarios = readSheetsTransporteUniversitarios().leer_datos_trabajo()
    connect_db().connect_db_transporte_universitarios(df_transporte_universitarios, host_dbb,user_dbb,pass_dbb,dbb_dwh)