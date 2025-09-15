"""
Entry point principal del sistema ETL
Permite ejecutar pipelines individuales o todas juntas
"""


import argparse
from src.runner import ETLRunner

def main():
    parser = argparse.ArgumentParser(
        description='Sistema ETL IPECD',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python -m src/main.py all                    # Ejecutar todas las pipelines
  python -m src.main sipa                  # Ejecutar solo SIPA
  python -m src/main.py --list                 # Listar pipelines disponibles
  python -m src/main.py --status               # Ver estado de pipelines
  python -m src/main.py all --force            # Forzar ejecución de todas
        """
    )
    
    parser.add_argument('pipeline', nargs='?', default='all', 
                       help='Pipeline a ejecutar (sipa, supermercado, semaforo, etc.) o "all" para todas')
    parser.add_argument('--force', action='store_true', 
                       help='Forzar ejecución aunque no haya datos nuevos')
    parser.add_argument('--list', action='store_true',
                       help='Listar todas las pipelines disponibles')
    parser.add_argument('--status', action='store_true',
                       help='Mostrar estado de todas las pipelines')
    
    args = parser.parse_args()
    
    runner = ETLRunner()
    
    # Manejar opciones de información
    if args.list:
        pipelines = runner.list_pipelines()
        print("📋 Pipelines disponibles:")
        for i, pipeline in enumerate(pipelines, 1):
            print(f"  {i:2d}. {pipeline}")
        return 0
    
    if args.status:
        print("🔍 Verificando estado de pipelines...")
        status = runner.get_pipeline_status()
        print("\n📊 Estado de pipelines:")
        for pipeline, available in status.items():
            status_icon = "✅" if available else "❌"
            print(f"  {status_icon} {pipeline}")
        return 0
    
    # Ejecutar pipelines
    if args.pipeline == 'all':
        success = runner.run_all_pipelines(force=args.force)
    else:
        success = runner.run_pipeline(args.pipeline, force=args.force)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()