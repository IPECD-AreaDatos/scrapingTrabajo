"""
MAIN - Orquestador ETL para Salud (Embarazo) 
"""
import os
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load
from etl.validate import Validate
from utils.logger import setup_logger

def main():
    setup_logger("salud_embarazo_etl")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL SALUD: EMBARAZO - %s ===", inicio)

    # Variables para PostgreSQL (Base Nueva)
    host, user, pwd, db = os.getenv('HOST_DBB2'), os.getenv('USER_DBB2'), os.getenv('PASSWORD_DBB2'), os.getenv('DB_NAME_SALUD')

    # Configuraciones de fuentes
    ID_DERIVACIONES = '1z4Z9-WWR9LF50xVQ4ALEU_VymVmSWQxFFDhAe8ZMDV8'
    ID_SUMAR = '10tBeHgb5ExBY62rdGM3uvmyd62gcDDS1XHbNr5u070E' 
    
    MESES_MAP = {'ENERO': '2026-01-01', 'FEBRERO': '2026-02-01', 'MARZO': '2026-03-01', 'ABRIL': '2026-04-01'}

    # Validación de variables críticas
    config = {'HOST': host, 'USER': user, 'PASS': pwd, 'DB': db}
    faltantes = [k for k, v in config.items() if not v]
    
    if faltantes:
        logger.error("Faltan variables de entorno: %s", faltantes)
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    extracter = Extract()
    transformer = Transform()
    validator = Validate()

    # --- FLUJO 1: DERIVACIONES ---
    try:
        logger.info("Extrayendo datos desde Google Sheets de derivaciones...")
        df_deriv_raw = extracter.extract_derivaciones(ID_DERIVACIONES, MESES_MAP)
        
        logger.info("Transformando datos de derivaciones...")
        df_deriv_clean = transformer.transform_derivaciones(df_deriv_raw)
        
        logger.info("Validando calidad de datos...")
        validator.validate(df_deriv_clean)
        
        logger.info("Cargando datos en PostgreSQL...")
        loader_deriv = Load(host, user, pwd, db)
        loader_deriv.tabla = "derivaciones_red_obstetricia"
        loader_deriv.load(df_deriv_clean)
        loader_deriv.close()

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
        
    except Exception as e:
        logger.error(f"Error en Fuente Derivaciones: {e}")
    
    # --- FLUJO 2: SUMAR ---
    try:
        logger.info("Extrayendo datos desde Google Sheets de sumar...")
        df_sumar_raw = extracter.extract_sumar(ID_SUMAR,'2026-03-01')
        
        logger.info("Transformando datos de sumar...")
        df_sumar_clean = transformer.transform_sumar(df_sumar_raw)

        print("\n" + "="*50)
        print("REPORTE TÉCNICO PRE-TRANSFORMACIÓN")
        print("="*50)
        print(f"Total de registros: {len(df_sumar_clean)}")        
        print(f"Total de columnas: {len(df_sumar_clean.columns)}")
            
        print("\n--- ANÁLISIS DE COLUMNAS ---")
            # Esto arma una tablita con nombre, tipo de dato y cuántos nulos hay
        info_columnas = pd.DataFrame({
                'Tipo': df_sumar_clean.dtypes,
                'No Nulos': df_sumar_clean.count(),
                'Nulos': df_sumar_clean.isnull().sum(),
                '% Completitud': (df_sumar_clean.count() / len(df_sumar_clean) * 100).round(2)
            })
        print(info_columnas)

        print("\n--- PRIMEROS 5 REGISTROS (VISTA PREVIA) ---")
        print(df_sumar_clean.head(15))
        print(df_sumar_clean.tail(15))
        print("="*50 + "\n")
        
        logger.info("Validando calidad de datos...")
        #validator.validate(df_sumar_clean)
        
        logger.info("Cargando datos en PostgreSQL...")
        loader_sumar = Load(host, user, pwd, db)
        loader_sumar.tabla = "embarazadas_sumar"
        loader_sumar.load(df_sumar_clean)
        loader_sumar.close()

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
        
    except Exception as e:
        logger.error(f"Error en Fuente SUMAR: {e}")

    

if __name__ == '__main__':
    main()
    exit()



    print("\n" + "="*50)
    print("REPORTE TÉCNICO PRE-TRANSFORMACIÓN")
    print("="*50)
    print(f"Total de registros: {len(df)}")        
    print(f"Total de columnas: {len(df.columns)}")
        
    print("\n--- ANÁLISIS DE COLUMNAS ---")
        # Esto arma una tablita con nombre, tipo de dato y cuántos nulos hay
    info_columnas = pd.DataFrame({
            'Tipo': df.dtypes,
            'No Nulos': df.count(),
            'Nulos': df.isnull().sum(),
            '% Completitud': (df.count() / len(df) * 100).round(2)
        })
    print(info_columnas)

    print("\n--- PRIMEROS 5 REGISTROS (VISTA PREVIA) ---")
    print(df.head(15))
    print(df.tail(15))
    print("="*50 + "\n")
# --- BLOQUE DE DEBUG PARA FECHAS NULAS ---
    nulos_fecha = df[df['fecha'].isna()]
    if not nulos_fecha.empty:
        print("\n" + "!"*30)
        print(f"REVISIÓN DE NULOS EN FECHA ({len(nulos_fecha)} filas)")
            # Mostramos el DNI o Apellido para que los ubiques en el Sheets
        print(nulos_fecha[['apellido_y_nombre', 'dni']].tail(10))
        print("!"*30 + "\n")
        # ----------------------------------------- 