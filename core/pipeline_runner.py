"""
Runner principal para ejecutar pipelines ETL
Maneja la orquestación, logging, registro de ejecuciones y manejo de errores
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from core.base_extractor import BaseExtractor
from core.base_transformer import BaseTransformer
from core.base_loader import BaseLoader
from config.settings import Settings


class PipelineRunner:
    """
    Orquestador principal para ejecutar pipelines ETL completos.
    
    Maneja:
    - Ejecución secuencial de Extract → Transform → Load
    - Logging centralizado
    - Registro de última ejecución
    - Manejo de errores y rollback
    """
    
    def __init__(self, module_name: str):
        """
        Inicializa el runner del pipeline.
        
        Args:
            module_name: Nombre del módulo ETL (debe coincidir con el nombre de carpeta)
        """
        self.module_name = module_name
        self.settings = Settings()
        self.logger = logging.getLogger(f"{__name__}.{module_name}")
        self.execution_history_file = self.settings.EXECUTION_HISTORY_FILE
        
        # Componentes del pipeline
        self.extractor: Optional[BaseExtractor] = None
        self.transformer: Optional[BaseTransformer] = None
        self.loader: Optional[BaseLoader] = None
        
        # Estado de ejecución
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_status: str = "pending"
        self.error_message: Optional[str] = None
    
    def set_components(
        self,
        extractor: BaseExtractor,
        transformer: BaseTransformer,
        loader: BaseLoader
    ) -> None:
        """
        Configura los componentes del pipeline.
        
        Args:
            extractor: Instancia del extractor
            transformer: Instancia del transformer
            loader: Instancia del loader
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.logger.info(f"[PIPELINE] Componentes configurados para {self.module_name}")
    
    def run(self, **kwargs) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL completo.
        
        Args:
            **kwargs: Argumentos adicionales para pasar a los componentes
            
        Returns:
            Diccionario con resultados de la ejecución:
            {
                'status': 'success' | 'failed',
                'duration_seconds': float,
                'extract_rows': int,
                'transform_rows': int,
                'load_success': bool,
                'error': str | None
            }
        """
        self.start_time = datetime.now()
        self.execution_status = "running"
        
        self.logger.info("=" * 80)
        self.logger.info(f"=== INICIO PIPELINE: {self.module_name.upper()} ===")
        self.logger.info(f"=== Fecha/Hora: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        self.logger.info("=" * 80)
        
        result = {
            'status': 'failed',
            'duration_seconds': 0.0,
            'extract_rows': 0,
            'transform_rows': 0,
            'load_success': False,
            'error': None,
        }
        
        extracted_data = None
        transformed_df = None
        
        try:
            # 1. EXTRACT
            self.logger.info("[PIPELINE] === FASE 1: EXTRACT ===")
            if not self.extractor:
                raise ValueError("Extractor no configurado")
            
            extracted_data = self.extractor.extract(**kwargs)
            
            if not self.extractor.validate_extraction(extracted_data):
                raise ValueError("Validación de extracción falló")
            
            if isinstance(extracted_data, dict) and 'data' in extracted_data:
                result['extract_rows'] = len(extracted_data.get('data', []))
            elif hasattr(extracted_data, '__len__'):
                result['extract_rows'] = len(extracted_data)
            
            self.logger.info(f"[PIPELINE] EXTRACT completado: {result['extract_rows']} registros")
            
            # 2. TRANSFORM
            self.logger.info("[PIPELINE] === FASE 2: TRANSFORM ===")
            if not self.transformer:
                raise ValueError("Transformer no configurado")
            
            transformed_df = self.transformer.transform(extracted_data, **kwargs)
            
            if not self.transformer.validate_transformation(transformed_df):
                raise ValueError("Validación de transformación falló")
            
            result['transform_rows'] = len(transformed_df)
            stats = self.transformer.get_transformation_stats(transformed_df)
            self.logger.info(
                f"[PIPELINE] TRANSFORM completado: {result['transform_rows']} filas, "
                f"{stats['columns']} columnas, {stats['memory_usage_mb']:.2f} MB"
            )
            
            # 3. LOAD
            self.logger.info("[PIPELINE] === FASE 3: LOAD ===")
            if not self.loader:
                raise ValueError("Loader no configurado")
            
            if not self.loader.validate_load_data(transformed_df):
                raise ValueError("Validación de datos para carga falló")
            
            load_success = self.loader.load(transformed_df, **kwargs)
            result['load_success'] = load_success
            
            if load_success:
                self.logger.info("[PIPELINE] LOAD completado exitosamente")
            else:
                self.logger.warning("[PIPELINE] LOAD completado pero sin nuevos datos")
            
            # Éxito
            self.execution_status = "success"
            result['status'] = 'success'
            
            self.logger.info("=" * 80)
            self.logger.info(f"=== PIPELINE COMPLETADO EXITOSAMENTE: {self.module_name.upper()} ===")
            
        except Exception as e:
            self.execution_status = "failed"
            self.error_message = str(e)
            result['error'] = str(e)
            
            self.logger.error("=" * 80)
            self.logger.error(f"=== ERROR EN PIPELINE: {self.module_name.upper()} ===")
            self.logger.error(f"Error: {e}", exc_info=True)
            self.logger.error("=" * 80)
            
            # Limpiar recursos en caso de error
            self._cleanup_on_error()
            
        finally:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            result['duration_seconds'] = duration
            
            # Registrar ejecución
            self._record_execution(result)
            
            # Limpiar recursos
            self._cleanup()
            
            self.logger.info(f"=== Duración total: {duration:.2f} segundos ===")
            self.logger.info("=" * 80)
        
        return result
    
    def _cleanup_on_error(self) -> None:
        """Limpia recursos cuando hay un error"""
        try:
            if self.extractor:
                self.extractor.cleanup()
            if self.loader:
                self.loader.close_connections()
        except Exception as e:
            self.logger.warning(f"Error durante limpieza: {e}")
    
    def _cleanup(self) -> None:
        """Limpia recursos al finalizar"""
        try:
            if self.extractor:
                self.extractor.cleanup()
            if self.loader:
                self.loader.close_connections()
        except Exception as e:
            self.logger.warning(f"Error durante limpieza: {e}")
    
    def _record_execution(self, result: Dict[str, Any]) -> None:
        """
        Registra la ejecución en el archivo de historial.
        
        Args:
            result: Resultado de la ejecución
        """
        try:
            # Cargar historial existente
            history = {}
            if self.execution_history_file.exists():
                with open(self.execution_history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            
            # Actualizar con nueva ejecución
            history[self.module_name] = {
                'last_execution': self.start_time.isoformat() if self.start_time else None,
                'status': self.execution_status,
                'duration_seconds': result['duration_seconds'],
                'extract_rows': result['extract_rows'],
                'transform_rows': result['transform_rows'],
                'load_success': result['load_success'],
                'error': result['error'],
            }
            
            # Guardar
            with open(self.execution_history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"[PIPELINE] Ejecución registrada en historial")
            
        except Exception as e:
            self.logger.warning(f"[PIPELINE] Error al registrar ejecución: {e}")
    
    def get_last_execution(self) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de la última ejecución del módulo.
        
        Returns:
            Diccionario con información de la última ejecución o None
        """
        try:
            if not self.execution_history_file.exists():
                return None
            
            with open(self.execution_history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
            
            return history.get(self.module_name)
            
        except Exception as e:
            self.logger.warning(f"Error al leer historial: {e}")
            return None



