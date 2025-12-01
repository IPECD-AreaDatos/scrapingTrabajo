"""
Pipeline ETL para Ventas de Combustible
Orquesta Extract → Transform → Load
"""
from core.pipeline_runner import PipelineRunner
from modules.ventas_combustible.extract import VentasCombustibleExtractor
from modules.ventas_combustible.transform import VentasCombustibleTransformer
from modules.ventas_combustible.load import VentasCombustibleLoader
from utils.logger import setup_logger


def main():
    """Función principal del pipeline"""
    # Configurar logging
    logger = setup_logger('ventas_combustible')
    
    # Crear runner
    runner = PipelineRunner('ventas_combustible')
    
    # Configurar componentes
    extractor = VentasCombustibleExtractor()
    transformer = VentasCombustibleTransformer()
    loader = VentasCombustibleLoader()
    
    runner.set_components(extractor, transformer, loader)
    
    try:
        # Ejecutar pipeline
        result = runner.run()
        
        # Si la transformación fue exitosa, calcular suma mensual y actualizar Google Sheets
        if result['status'] == 'success' and result['transform_rows'] > 0:
            # Obtener datos transformados del último run
            # Nota: En una versión futura, el runner podría exponer los datos transformados
            # Por ahora, necesitamos re-transformar para obtener la suma
            try:
                extracted_data = extractor.extract()
                transformed_df = transformer.transform(extracted_data)
                suma_mensual = transformer.calcular_suma_por_fecha(transformed_df)
                
                # Actualizar Google Sheets si hubo carga exitosa
                if result['load_success']:
                    loader.load(transformed_df, suma_mensual=suma_mensual)
            except Exception as e:
                logger.warning(f"Error al actualizar Google Sheets: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error en pipeline: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

