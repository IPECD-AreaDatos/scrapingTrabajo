import pandas as pd
import os
from dotenv import load_dotenv
from pymysql import connect

def verificar_carga():
    """Verificar que los datos se cargaron correctamente"""
    
    # Cargar variables de entorno
    load_dotenv()
    
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DWH_SOCIO')
    
    try:
        # Conectar a la base de datos
        conn = connect(host=host, user=user, password=password, database=database)
        
        print("🔍 Verificando datos cargados en la tabla rama_actividad_economica...")
        
        # Contar registros totales
        query_count = "SELECT COUNT(*) as total FROM rama_actividad_economica"
        df_count = pd.read_sql(query_count, conn)
        print(f"📊 Total de registros en la tabla: {df_count['total'].iloc[0]:,}")
        
        # Mostrar algunos registros de muestra
        query_sample = "SELECT * FROM rama_actividad_economica LIMIT 5"
        df_sample = pd.read_sql(query_sample, conn)
        print("\n📋 Muestra de los primeros 5 registros:")
        print(df_sample.to_string(index=False))
        
        # Verificar rangos de código
        query_stats = """
        SELECT 
            MIN(codigo) as codigo_min,
            MAX(codigo) as codigo_max,
            COUNT(DISTINCT codigo) as codigos_unicos
        FROM rama_actividad_economica
        """
        df_stats = pd.read_sql(query_stats, conn)
        print(f"\n📈 Estadísticas:")
        print(f"   - Código mínimo: {df_stats['codigo_min'].iloc[0]:,}")
        print(f"   - Código máximo: {df_stats['codigo_max'].iloc[0]:,}")
        print(f"   - Códigos únicos: {df_stats['codigos_unicos'].iloc[0]:,}")
        
        # Verificar suma de algunas columnas
        query_sums = """
        SELECT 
            SUM(comercio) as total_comercio,
            SUM(industria) as total_industria,
            SUM(agropecuaria_pesca_o_mineria) as total_agropecuaria
        FROM rama_actividad_economica
        """
        df_sums = pd.read_sql(query_sums, conn)
        print(f"\n🔢 Totales por sector:")
        print(f"   - Comercio: {df_sums['total_comercio'].iloc[0]:,}")
        print(f"   - Industria: {df_sums['total_industria'].iloc[0]:,}")
        print(f"   - Agropecuaria/Pesca/Minería: {df_sums['total_agropecuaria'].iloc[0]:,}")
        
        conn.close()
        print("\n✅ Verificación completada exitosamente")
        
    except Exception as e:
        print(f"❌ Error durante la verificación: {e}")

if __name__ == "__main__":
    verificar_carga()