"""
VALIDATE - Módulo de validación de datos RIPTE
"""
import logging

logger = logging.getLogger(__name__)


class ValidateRIPTE:
    def validate(self, valor: float):
        if valor is None:
            raise ValueError("[VALIDATE] Valor RIPTE es None.")
        if valor <= 0:
            raise ValueError(f"[VALIDATE] Valor RIPTE inválido: {valor}")
        logger.info("[VALIDATE] OK — RIPTE: %f", valor)
