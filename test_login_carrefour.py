#!/usr/bin/env python3
"""
Prueba rápida del login de Carrefour
"""
import sys
import os

# Agregar el directorio padre al path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_login():
    """Prueba solo el login de Carrefour"""
    try:
        from canasta_basica.carrefour_extractor import ExtractorCarrefour
        
        print("🚀 PROBANDO LOGIN DE CARREFOUR")
        print("=" * 50)
        
        # Crear extractor
        extractor = ExtractorCarrefour()
        
        # Solo probar login
        if extractor.asegurar_sesion_activa():
            print("✅ LOGIN EXITOSO")
        else:
            print("❌ LOGIN FALLÓ")
            
        # Cerrar driver
        extractor.cerrar()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_login()