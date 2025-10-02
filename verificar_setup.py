#!/usr/bin/env python3
"""
Script de verificación de dependencias y configuración
"""
import os
import sys
from dotenv import load_dotenv

def verificar_dependencias():
    """Verifica que todas las dependencias estén instaladas"""
    print("🔍 Verificando dependencias...")
    
    dependencias = [
        ('selenium', 'Selenium WebDriver'),
        ('pandas', 'Pandas para manejo de datos'),
        ('pymysql', 'PyMySQL para conexión a BD'),
        ('sqlalchemy', 'SQLAlchemy para ORM'),
        ('google-api-python-client', 'Google API Client'),
        ('google-auth', 'Google Auth'),
        ('python-dotenv', 'Python dotenv')
    ]
    
    faltantes = []
    for paquete, descripcion in dependencias:
        try:
            __import__(paquete.replace('-', '_'))
            print(f"✅ {descripcion}")
        except ImportError:
            print(f"❌ {descripcion} - FALTANTE")
            faltantes.append(paquete)
    
    if faltantes:
        print(f"\n⚠️ Paquetes faltantes: {', '.join(faltantes)}")
        print("💡 Instálalos con: pip install " + " ".join(faltantes))
        return False
    
    print("✅ Todas las dependencias están disponibles")
    return True

def verificar_configuracion():
    """Verifica la configuración del entorno"""
    print("\n🔍 Verificando configuración...")
    
    # Cargar variables de entorno
    load_dotenv()
    
    # Variables críticas
    variables_criticas = [
        'HOST_DBB',
        'USER_DBB', 
        'PASSWORD_DBB',
        'NAME_DBB_DATALAKE_ECONOMICO'
    ]
    
    # Variables opcionales
    variables_opcionales = [
        'CARREFOUR_EMAIL',
        'CARREFOUR_PASSWORD',
        'SHEETS_RANGE',
        'GOOGLE_SHEETS_CREDENTIALS_FILE',
        'GOOGLE_SHEETS_CREDENTIALS'
    ]
    
    # Verificar críticas
    faltantes_criticas = []
    for var in variables_criticas:
        valor = os.getenv(var)
        if valor:
            print(f"✅ {var}: configurada")
        else:
            print(f"❌ {var}: FALTANTE")
            faltantes_criticas.append(var)
    
    # Verificar opcionales
    print("\n📋 Variables opcionales:")
    for var in variables_opcionales:
        valor = os.getenv(var)
        if valor:
            print(f"✅ {var}: configurada")
        else:
            print(f"⚠️ {var}: no configurada (usar .env.example como referencia)")
    
    if faltantes_criticas:
        print(f"\n❌ Variables críticas faltantes: {', '.join(faltantes_criticas)}")
        print("💡 Configúralas en el archivo .env")
        return False
    
    print("\n✅ Configuración básica correcta")
    return True

def verificar_archivos():
    """Verifica que los archivos necesarios existan"""
    print("\n🔍 Verificando estructura de archivos...")
    
    archivos_requeridos = [
        'canasta_basica/__init__.py',
        'canasta_basica/run.py',
        'canasta_basica/carrefour_extractor.py',
        'canasta_basica/load.py',
        'canasta_basica/utils_db.py',
        'canasta_basica/utils_sheets.py'
    ]
    
    faltantes = []
    for archivo in archivos_requeridos:
        if os.path.exists(archivo):
            print(f"✅ {archivo}")
        else:
            print(f"❌ {archivo} - FALTANTE")
            faltantes.append(archivo)
    
    if faltantes:
        print(f"\n❌ Archivos faltantes: {len(faltantes)}")
        return False
    
    print("\n✅ Todos los archivos están presentes")
    return True

def main():
    """Función principal de verificación"""
    print("=" * 60)
    print("🔧 VERIFICACIÓN DEL PIPELINE CANASTA BÁSICA")
    print("=" * 60)
    
    resultados = []
    
    # Verificar dependencias
    resultados.append(verificar_dependencias())
    
    # Verificar configuración
    resultados.append(verificar_configuracion())
    
    # Verificar archivos
    resultados.append(verificar_archivos())
    
    # Resultado final
    print("\n" + "=" * 60)
    if all(resultados):
        print("✅ SISTEMA LISTO PARA EJECUTAR")
        print("💡 Ejecuta: python test_canasta_basica.py")
    else:
        print("❌ SISTEMA NO ESTÁ LISTO")
        print("💡 Corrige los problemas indicados arriba")
    print("=" * 60)

if __name__ == "__main__":
    main()