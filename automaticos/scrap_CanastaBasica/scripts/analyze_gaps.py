import os
import sys
from dotenv import load_dotenv

# Agregar directorio raíz del proyecto para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.load import LoadCanastaBasica

def main():
    load_dotenv()
    loader = LoadCanastaBasica()
    
    print("=== ANÁLISIS DE PERIODICIDAD DE CARGAS ===")
    
    if loader.db_v2.connect_db():
        try:
            # Obtener las últimas 10 fechas con carga
            query = """
                SELECT fecha_extraccion, count(*) 
                FROM precios_productos 
                GROUP BY fecha_extraccion 
                ORDER BY fecha_extraccion DESC 
                LIMIT 15
            """
            res = loader.db_v2.execute_query(query)
            
            if res:
                print("\nÚltimas fechas registradas:")
                print(f"{'Fecha':<15} | {'Registros':<10}")
                print("-" * 30)
                
                fechas = []
                for row in res:
                    fecha = row[0]
                    cantidad = row[1]
                    print(f"{str(fecha):<15} | {cantidad:<10}")
                    fechas.append(fecha)
                
                # Calcular huecos
                print("\nAnálisis de huecos (Gaps):")
                from datetime import timedelta
                
                for i in range(len(fechas) - 1):
                    gap = (fechas[i] - fechas[i+1]).days
                    if gap > 1:
                        print(f" - ¡HUECO DETECTADO! Entre {fechas[i+1]} y {fechas[i]} pasaron {gap} días.")
                    else:
                        print(f" - Carga consecutiva entre {fechas[i+1]} y {fechas[i]}.")
            else:
                print("No se encontraron registros en la tabla precios_productos.")
                
        except Exception as e:
            print(f"Error analizando fechas: {e}")
        finally:
            loader.db_v2.close_connections()
    else:
        print("No se pudo conectar a la base de datos para el análisis.")

if __name__ == "__main__":
    main()
