"""
API principal FastAPI para gestión de pipelines ETL
Endpoints para ejecutar pipelines, consultar estado y logs
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import json
import subprocess
import sys

from config.settings import Settings
from core.pipeline_runner import PipelineRunner
from utils.logger import setup_logger

# Inicializar FastAPI
app = FastAPI(
    title="ETL Pipelines API",
    description="API para gestión y ejecución de pipelines ETL",
    version="1.0.0"
)

settings = Settings()
logger = setup_logger('web_api')


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "ETL Pipelines API",
        "version": "1.0.0",
        "endpoints": {
            "modules": "/api/modules",
            "execute": "/api/modules/{module_name}/execute",
            "status": "/api/modules/{module_name}/status",
            "logs": "/api/modules/{module_name}/logs"
        }
    }


@app.get("/api/modules")
async def list_modules() -> Dict[str, List[str]]:
    """
    Lista todos los módulos ETL disponibles.
    
    Returns:
        Lista de nombres de módulos
    """
    modules_dir = settings.MODULES_DIR
    if not modules_dir.exists():
        return {"modules": []}
    
    modules = [
        d.name for d in modules_dir.iterdir()
        if d.is_dir() and (d / 'pipeline.py').exists()
    ]
    
    return {"modules": sorted(modules)}


@app.get("/api/modules/{module_name}/status")
async def get_module_status(module_name: str) -> Dict[str, Any]:
    """
    Obtiene el estado de la última ejecución de un módulo.
    
    Args:
        module_name: Nombre del módulo
        
    Returns:
        Información de la última ejecución
    """
    try:
        runner = PipelineRunner(module_name)
        last_execution = runner.get_last_execution()
        
        if not last_execution:
            return {
                "module": module_name,
                "status": "never_executed",
                "last_execution": None
            }
        
        return {
            "module": module_name,
            "status": last_execution.get('status', 'unknown'),
            "last_execution": last_execution.get('last_execution'),
            "duration_seconds": last_execution.get('duration_seconds'),
            "extract_rows": last_execution.get('extract_rows'),
            "transform_rows": last_execution.get('transform_rows'),
            "load_success": last_execution.get('load_success'),
            "error": last_execution.get('error')
        }
    except Exception as e:
        logger.error(f"Error al obtener estado: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/modules/{module_name}/execute")
async def execute_module(
    module_name: str,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Ejecuta un módulo ETL.
    
    Args:
        module_name: Nombre del módulo a ejecutar
        background_tasks: Tareas en background de FastAPI
        
    Returns:
        Confirmación de inicio de ejecución
    """
    # Verificar que el módulo existe
    module_path = settings.MODULES_DIR / module_name / 'pipeline.py'
    if not module_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Módulo '{module_name}' no encontrado"
        )
    
    # Ejecutar en background
    background_tasks.add_task(_run_pipeline, module_name)
    
    return {
        "message": f"Pipeline {module_name} iniciado",
        "module": module_name,
        "timestamp": datetime.now().isoformat()
    }


def _run_pipeline(module_name: str) -> None:
    """
    Ejecuta un pipeline en background.
    
    Args:
        module_name: Nombre del módulo
    """
    try:
        logger.info(f"Ejecutando pipeline {module_name} en background")
        
        # Ejecutar como subprocess para mejor aislamiento
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                f"modules.{module_name}.pipeline"
            ],
            cwd=str(settings.BASE_DIR),
            capture_output=True,
            text=True,
            timeout=3600  # 1 hora máximo
        )
        
        if result.returncode != 0:
            logger.error(f"Error en pipeline {module_name}: {result.stderr}")
        else:
            logger.info(f"Pipeline {module_name} completado exitosamente")
            
    except subprocess.TimeoutExpired:
        logger.error(f"Pipeline {module_name} excedió el tiempo límite")
    except Exception as e:
        logger.error(f"Error al ejecutar pipeline {module_name}: {e}")


@app.get("/api/modules/{module_name}/logs")
async def get_module_logs(
    module_name: str,
    lines: int = 100
) -> Dict[str, Any]:
    """
    Obtiene los últimos logs de un módulo.
    
    Args:
        module_name: Nombre del módulo
        lines: Número de líneas a retornar (default: 100)
        
    Returns:
        Logs del módulo
    """
    log_file = settings.LOGS_DIR / f"{module_name}.log"
    
    if not log_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Logs no encontrados para módulo '{module_name}'"
        )
    
    try:
        # Leer últimas N líneas
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        return {
            "module": module_name,
            "log_file": str(log_file),
            "total_lines": len(all_lines),
            "returned_lines": len(last_lines),
            "logs": ''.join(last_lines)
        }
    except Exception as e:
        logger.error(f"Error al leer logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



