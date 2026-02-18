"""
TRANSFORM - Módulo de transformación de datos DOLAR
Responsabilidad: Validar y normalizar los DataFrames de cotizaciones
(los datos ya vienen limpios del extract, este módulo es un pass-through con logging)
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TransformDOLAR:
    """Valida y normaliza los DataFrames de cotizaciones de dólar."""

    def transform(self, datos: dict) -> dict:
        """
        Valida que los DataFrames tengan los campos correctos.

        Args:
            datos: dict con {'oficial': df, 'blue': df, 'mep': df, 'ccl': df}

        Returns:
            dict: mismo formato, datos validados
        """
        logger.info("[TRANSFORM] Validando %d tipos de dólar...", len(datos))
        for tipo, df in datos.items():
            if df is None or df.empty:
                logger.warning("[TRANSFORM] DataFrame vacío para dólar %s", tipo)
                continue
            logger.info("[TRANSFORM] %s — fecha: %s | compra: %s | venta: %s",
                        tipo, df['fecha'].iloc[0], df['compra'].iloc[0], df['venta'].iloc[0])
        return datos
