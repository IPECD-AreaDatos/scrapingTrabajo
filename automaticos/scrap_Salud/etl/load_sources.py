"""
Script de carga de datos desde los 4 archivos Excel fuente.
Ejecutar desde la raíz del proyecto con el venv activado:
    python etl/load_sources.py

Cada fuente se carga en orden. El proceso es idempotente:
si un registro (dni, fpp) ya existe, se actualiza en lugar de duplicarse.
"""

import os
import sys
import logging
from dotenv import load_dotenv
from app.db_config import get_database_url

load_dotenv()

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from etl.ingestion import load_excel_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

DB_URL = get_database_url()
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

# ── Configuración de fuentes ──────────────────────────────────────────────────
# Ajustar rutas y nombres de hoja según los archivos disponibles en /data.
# La clave 'fuente' debe coincidir exactamente con field_map.py.

SOURCES = [
    {
        "fuente":    "pof",
        "file_path": os.path.join("data", "20260410_EmbarazosEnCurso_POF.xlsx"),
        "sheet":     "embarazadas",
    },
    {
        "fuente":    "wp",
        "file_path": os.path.join("data", "WPListadoGeneralEmbarazos.xlsx"),
        "sheet":     "embarazadas",
    },
    {
        "fuente":    "derivaciones",
        "file_path": os.path.join("data", "DERIVACIONES 2026 RED obstetrica AR (1) (1).xlsx"),
        "sheet":     "ENERO/FEBRERO/MARZO/ABRIL",
    },
    {
        "fuente":    "sumar",
        "file_path": os.path.join("data", "EmbarazadasSUMAR.xlsx"),
        "sheet":     "Embarazadas",
    },
]


def run_all():
    session = Session()
    total = {"insertados": 0, "actualizados": 0, "errores": 0}

    print("\n══════════════════════════════════════════════════")
    print("  INGESTA DE FUENTES — Sistema de Seguimiento Obstétrico")
    print("══════════════════════════════════════════════════\n")

    for src in SOURCES:
        fuente    = src["fuente"]
        file_path = src["file_path"]
        sheet     = src["sheet"]

        if not os.path.exists(file_path):
            print(f"  ⚠  [{fuente.upper()}] Archivo no encontrado: {file_path} — omitido.\n")
            continue

        print(f"  ▶  Procesando fuente: {fuente.upper()}")
        try:
            result = load_excel_source(session, file_path, sheet, fuente)
            for k in total:
                total[k] += result[k]
            print(f"     ✓ +{result['insertados']} nuevos | ~{result['actualizados']} actualizados | ✗{result['errores']} errores\n")
        except Exception as e:
            print(f"     ✗ Error en {fuente.upper()}: {e}\n")

    session.close()

    print("══════════════════════════════════════════════════")
    print(f"  TOTAL: +{total['insertados']} insertados | ~{total['actualizados']} actualizados | ✗{total['errores']} errores")
    print("══════════════════════════════════════════════════\n")


if __name__ == "__main__":
    run_all()
