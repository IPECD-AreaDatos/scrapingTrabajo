"""
TRANSFORM - Módulo de transformación de datos RIPTE
Responsabilidad: Convertir el valor string a float
"""
import logging

logger = logging.getLogger(__name__)


class TransformRIPTE:
    """Transforma el valor RIPTE de string a float."""

    def transform(self, valor_str: str) -> float:
        """
        Convierte el valor a float.

        Returns:
            float: Valor numérico del RIPTE
        """
        try:
            valor = float(valor_str)
            logger.info("[TRANSFORM] Valor RIPTE: %f", valor)
            return valor
        except (ValueError, TypeError) as e:
            raise ValueError(f"[TRANSFORM] No se pudo convertir el valor RIPTE '{valor_str}': {e}")
