"""
Main - Orquestador del flujo ETL para CBT/CBA

Este script coordina el proceso completo de:
1. Extract: Descarga de archivos desde INDEC
2. Transform: Procesamiento y transformación de datos
3. Load: Carga a base de datos y envío de correos
4. Validate: Validación de calidad de datos
"""

import os
import sys
from dotenv import load_dotenv

# Importar componentes ETL
from extract.extractor_cbt import ExtractorCBT
from extract.extractor_pobreza import ExtractorPobreza
from transform.transformer_cbt_cba import TransformerCBTCBA
from load.database_loader import connection_db
from load.email_sender import MailCBTCBA
from validate.data_validator import DataValidator

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos
HOST_DBB = os.getenv('HOST_DBB')
USER_DBB = os.getenv('USER_DBB')
PASS_DBB = os.getenv('PASSWORD_DBB')
DBB_DATALAKE = os.getenv('NAME_DBB_DATALAKE_SOCIO')
DBB_DWH = os.getenv('NAME_DBB_DWH_SOCIO')


def main():
    """
    Función principal que ejecuta el flujo ETL completo.
    """
    print("\n" + "="*70)
    print("INICIO DEL PROCESO ETL - CBT/CBA")
    print("="*70 + "\n")
    
    try:
        # ============================================================
        # FASE 1: EXTRACT - Extracción de datos
        # ============================================================
        print("\n[FASE 1/4] EXTRACT - Extracción de datos")
        print("-" * 70)
        
        # Descargar archivo CBT
        print("\n1.1. Descargando archivo CBT...")
        extractor_cbt = ExtractorCBT()
        ruta_cbt = extractor_cbt.descargar_archivo()
        
        # Descargar archivo Pobreza
        print("\n1.2. Descargando archivo Pobreza...")
        extractor_pobreza = ExtractorPobreza()
        ruta_pobreza = extractor_pobreza.descargar_archivo()
        
        print("\n✓ Fase de extracción completada exitosamente")
        
        # ============================================================
        # FASE 2: TRANSFORM - Transformación de datos
        # ============================================================
        print("\n[FASE 2/4] TRANSFORM - Transformación de datos")
        print("-" * 70)
        
        print("\n2.1. Procesando datos de CBT, CBA y NEA...")
        transformer = TransformerCBTCBA()
        df_transformado = transformer.transform_datalake()
        
        print(f"\n✓ Datos transformados: {len(df_transformado)} registros")
        print(f"  Columnas: {', '.join(df_transformado.columns.tolist())}")
        print(f"  Rango de fechas: {df_transformado['Fecha'].min()} a {df_transformado['Fecha'].max()}")
        
        # ============================================================
        # FASE 3: VALIDATE - Validación de datos
        # ============================================================
        print("\n[FASE 3/4] VALIDATE - Validación de datos")
        print("-" * 70)
        
        validator = DataValidator()
        es_valido, errores, advertencias = validator.validar_dataframe(df_transformado)
        
        # Mostrar reporte de validación
        print(validator.generar_reporte())
        
        if not es_valido:
            print("\n✗ VALIDACIÓN FALLIDA - No se procederá con la carga")
            print("\nErrores encontrados:")
            for i, error in enumerate(errores, 1):
                print(f"  {i}. {error}")
            sys.exit(1)
        
        print("✓ Validación completada exitosamente")
        
        # ============================================================
        # FASE 4: LOAD - Carga de datos
        # ============================================================
        print("\n[FASE 4/4] LOAD - Carga de datos")
        print("-" * 70)
        
        # Cargar a DataLake
        print("\n4.1. Cargando datos al DataLake...")
        db_loader = connection_db(HOST_DBB, USER_DBB, PASS_DBB, DBB_DATALAKE)
        db_loader.connect_db()
        bandera_correo = db_loader.load_datalake(df_transformado)
        
        # Enviar correo si se cargaron datos nuevos
        if bandera_correo:
            print("\n4.2. Enviando correo de notificación...")
            email_sender = MailCBTCBA(HOST_DBB, USER_DBB, PASS_DBB, DBB_DWH)
            email_sender.send_mail_cbt_cba()
            print("✓ Correo enviado exitosamente")
            
            # Llamada a API
            print("\n4.3. Enviando datos a la API...")
            import requests
            
            # Filtrar la última fila
            last_row = df_transformado.tail(1).iloc[0]
            
            # Extraer los valores necesarios
            anio = last_row['Fecha'].year
            mes = last_row['Fecha'].month
            cbt = last_row['cbt_nea']
            cba = last_row['cba_nea']
            
            # Endpoint
            url = "https://ecv.corrientes.gob.ar/api/create_cbt"
            
            # Datos a enviar
            data = {
                "anio": anio,
                "mes": mes,
                "cbt": int(cbt),
                "cba": int(cba)
            }
            
            # Realizar solicitud POST
            response = requests.post(url, data=data)
            
            if response.status_code == 200:
                print(f"✓ API actualizada exitosamente: {response.json()}")
            else:
                print(f"⚠ Error en la API: {response.status_code}")
                print(f"  Respuesta: {response.text}")
        else:
            print("\n⚠ No hay datos nuevos para cargar")
        
        print("\n✓ Fase de carga completada")
        
        # ============================================================
        # RESUMEN FINAL
        # ============================================================
        print("\n" + "="*70)
        print("PROCESO ETL COMPLETADO EXITOSAMENTE")
        print("="*70)
        print(f"\n📊 Resumen:")
        print(f"  • Registros procesados: {len(df_transformado)}")
        print(f"  • Última fecha: {df_transformado['Fecha'].max()}")
        print(f"  • Validaciones: {'✓ Aprobadas' if es_valido else '✗ Fallidas'}")
        print(f"  • Advertencias: {len(advertencias)}")
        print(f"  • Datos nuevos cargados: {'Sí' if bandera_correo else 'No'}")
        print("\n" + "="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ ERROR EN EL PROCESO ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
