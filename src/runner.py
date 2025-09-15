"""
Orquestador de pipelines ETL
Maneja la ejecución individual y masiva de pipelines
"""

import logging
from typing import Dict, List, Optional
from src.shared.logger import setup_logger

class ETLRunner:
    def __init__(self):
        self.logger = setup_logger("etl_runner")
        self.pipelines = {
            'sipa': self._run_sipa,
            'supermercado': self._run_supermercado,
            'semaforo': self._run_semaforo,
            'emae': self._run_emae,
            'dnrpa': self._run_dnrpa,
            'anac': self._run_anac,
            'combustible': self._run_combustible,
            'canasta_basica': self._run_canasta_basica,
            'ipicorr': self._run_ipicorr,
            'mercado_central': self._run_mercado_central,
            'ripte': self._run_ripte,
        }
    
    def run_pipeline(self, pipeline_name: str, force: bool = False) -> bool:
        """Ejecuta una pipeline específica"""
        if pipeline_name not in self.pipelines:
            self.logger.error(f"Pipeline '{pipeline_name}' no encontrada")
            return False
        
        try:
            self.logger.info(f"🚀 Iniciando pipeline: {pipeline_name}")
            result = self.pipelines[pipeline_name](force=force)
            self.logger.info(f"✅ Pipeline {pipeline_name} completada exitosamente")
            return result
        except Exception as e:
            self.logger.error(f"❌ Error en pipeline {pipeline_name}: {e}")
            return False
    
    def run_all_pipelines(self, force: bool = False) -> bool:
        """Ejecuta todas las pipelines en secuencia"""
        success_count = 0
        total_pipelines = len(self.pipelines)
        
        self.logger.info(f"🔄 Ejecutando {total_pipelines} pipelines...")
        
        for pipeline_name in self.pipelines.keys():
            if self.run_pipeline(pipeline_name, force):
                success_count += 1
            else:
                self.logger.warning(f"⚠️ Pipeline {pipeline_name} falló, continuando...")
        
        self.logger.info(f"📊 Resumen: {success_count}/{total_pipelines} pipelines exitosas")
        return success_count == total_pipelines
    
    def _run_sipa(self, force: bool = False) -> bool:
        from src.pipelines.sipa.run import run_sipa
        return run_sipa()
    
    def _run_supermercado(self, force: bool = False) -> bool:
        from src.pipelines.supermercado.run import run_supermercado
        return run_supermercado()
    
    def _run_semaforo(self, force: bool = False) -> bool:
        from src.pipelines.semaforo.run import run_semaforo
        return run_semaforo()
    
    # Agregar métodos para las demás pipelines...
    def _run_emae(self, force: bool = False) -> bool:
        from src.pipelines.emae.run import run_emae
        return run_emae()
    
    def _run_dnrpa(self, force: bool = False) -> bool:
        from src.pipelines.dnrpa.run import run_dnrpa
        return run_dnrpa()
    
    def _run_anac(self, force: bool = False) -> bool:
        from src.pipelines.anac.run import run_anac
        return run_anac()
    
    def _run_combustible(self, force: bool = False) -> bool:
        from src.pipelines.combustible.run import run_combustible
        return run_combustible()
    
    def _run_canasta_basica(self, force: bool = False) -> bool:
        from src.pipelines.canasta_basica.run import run_canasta_basica
        return run_canasta_basica()
    
    def _run_ipicorr(self, force: bool = False) -> bool:
        from src.pipelines.ipicorr.run import run_ipicorr
        return run_ipicorr()
    
    def _run_mercado_central(self, force: bool = False) -> bool:
        from src.pipelines.mercado_central.run import run_mercado_central
        return run_mercado_central()
    
    def _run_ripte(self, force: bool = False) -> bool:
        from src.pipelines.ripte.run import run_ripte
        return run_ripte()
    
    def list_pipelines(self) -> List[str]:
        """Lista todas las pipelines disponibles"""
        return list(self.pipelines.keys())
    
    def get_pipeline_status(self) -> Dict[str, bool]:
        """Verifica qué pipelines están disponibles para ejecutar"""
        status = {}
        for pipeline_name in self.pipelines.keys():
            try:
                # Intenta importar cada pipeline para verificar si está disponible
                if pipeline_name == 'sipa':
                    from src.pipelines.sipa.run import run_sipa
                elif pipeline_name == 'supermercado':
                    from src.pipelines.supermercado.run import run_supermercado
                elif pipeline_name == 'semaforo':
                    from src.pipelines.semaforo.run import run_semaforo
                elif pipeline_name == 'emae':
                    from src.pipelines.emae.run import run_emae
                elif pipeline_name == 'dnrpa':
                    from src.pipelines.dnrpa.run import run_dnrpa
                elif pipeline_name == 'anac':
                    from src.pipelines.anac.run import run_anac
                elif pipeline_name == 'combustible':
                    from src.pipelines.combustible.run import run_combustible
                elif pipeline_name == 'canasta_basica':
                    from src.pipelines.canasta_basica.run import run_canasta_basica
                elif pipeline_name == 'ipicorr':
                    from src.pipelines.ipicorr.run import run_ipicorr
                elif pipeline_name == 'mercado_central':
                    from src.pipelines.mercado_central.run import run_mercado_central
                elif pipeline_name == 'ripte':
                    from src.pipelines.ripte.run import run_ripte
                status[pipeline_name] = True
            except ImportError as e:
                self.logger.warning(f"Pipeline {pipeline_name} no disponible: {e}")
                status[pipeline_name] = False
        return status

