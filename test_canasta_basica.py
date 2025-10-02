#!/usr/bin/env python3
"""
Script de prueba para el pipeline de canasta básica
"""
import sys
import os

# Agregar el directorio padre al path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_pipeline():
    """Prueba básica del pipeline"""
    try:
        from canasta_basica.run import run_canasta_basica
        
        print("=" * 60)
        print("🚀 INICIANDO PRUEBA DEL PIPELINE CANASTA BÁSICA")
        print("=" * 60)
        
        # Ejecutar pipeline
        run_canasta_basica()
        
        print("=" * 60)
        print("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        
    except ImportError as e:
        print(f"❌ Error de import: {e}")
        print("💡 Asegúrate de que todas las dependencias estén instaladas")
        
    except Exception as e:
        print(f"❌ Error durante ejecución: {e}")
        print("💡 Revisa los logs para más detalles")

if __name__ == "__main__":
    test_pipeline()