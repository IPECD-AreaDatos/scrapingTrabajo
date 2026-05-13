import os
import sys
from dotenv import load_dotenv

# Agregar directorio raíz del proyecto para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.load import LoadCanastaBasica

def main():
    load_dotenv()
    loader = LoadCanastaBasica()
    
    from datetime import datetime
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    
    bases = [
        (loader.db_v1, "BASE VIEJA (MySQL)"),
        (loader.db_v2, "BASE NUEVA (Postgres)")
    ]
    
    print(f"=== VERIFICACIÓN DE CARGA - FECHA: {fecha_hoy} ===")
    
    for db, nombre in bases:
        print(f"\nConsultando {nombre}...")
        if db.connect_db():
            try:
                # 1. Conteo total
                query_total = f"SELECT count(*) FROM precios_productos WHERE fecha_extraccion = '{fecha_hoy}'"
                res_total = db.execute_query(query_total)
                count = res_total[0][0] if res_total else 0
                print(f" - Total registros hoy: {count}")
                
                if count > 0:
                    # 2. Desglose por supermercado
                    query_sm = f"SELECT supermercado, count(*) FROM precios_productos WHERE fecha_extraccion = '{fecha_hoy}' GROUP BY supermercado"
                    res_sm = db.execute_query(query_sm)
                    print(" - Desglose:")
                    for row in res_sm:
                        print(f"   * {row[0]}: {row[1]}")
                else:
                    print(" - No se encontraron registros para esta fecha.")
            except Exception as e:
                print(f" - Error consultando {nombre}: {e}")
            finally:
                db.close_connections()
        else:
            print(f" - No se pudo conectar a {nombre}")

if __name__ == "__main__":
    main()
