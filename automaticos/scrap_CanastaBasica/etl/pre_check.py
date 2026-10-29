"""
Módulo de Pre-validación de conectividad y salud de links (Health Check / Circuit Breaker)
Responsabilidad: Verificar accesibilidad de fuentes antes de iniciar el scraping masivo.
"""
import random
import logging
import requests
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9',
}


class PreCheckCanastaBasica:
    def __init__(self, sample_size: int = 10, timeout: int = 8):
        self.sample_size = sample_size
        self.timeout = timeout

    def check_link_health(self, url: str) -> bool:
        try:
            response = requests.head(url, headers=HEADERS, timeout=self.timeout, allow_redirects=True)
            if response.status_code < 400:
                return True
            if response.status_code in [403, 405]:
                resp_get = requests.get(url, headers=HEADERS, timeout=self.timeout, stream=True)
                return resp_get.status_code < 400
            return False
        except requests.RequestException:
            return False

    def validate_sources(self, links_list: List[Dict]) -> Tuple[List[Dict], List[str], str]:
        """
        Verifica una muestra sin semillado por cada supermercado.
        Retorna:
            - links_validos (list)
            - supers_caidos (list)
            - texto_observaciones (str para guardar en SQL)
        """
        logger.info("=" * 60)
        logger.info("[PRE-CHECK] Iniciando verificación previa de conectividad...")
        logger.info("[PRE-CHECK] Muestra aleatoria sin semillado: %d links por comercio.", self.sample_size)
        logger.info("=" * 60)

        super_map: Dict[str, List[Dict]] = {}
        for item in links_list:
            s_name = item.get('supermercado') or item.get('nombre_super') or 'Desconocido'
            super_map.setdefault(s_name, []).append(item)

        total_supers = len(super_map)
        supers_caidos = []
        supers_activos = []

        for super_name, items in super_map.items():
            n_sample = min(len(items), self.sample_size)
            muestra = random.sample(items, n_sample)

            exitos = 0
            for link_item in muestra:
                url = link_item.get('url') or link_item.get('link')
                if url and self.check_link_health(url):
                    exitos += 1

            tasa_exito = (exitos / n_sample) * 100
            logger.info(
                f"[PRE-CHECK] {super_name}: {exitos}/{n_sample} links respondieron correctamente ({tasa_exito:.1f}%)."
            )

            if exitos == 0:
                supers_caidos.append(super_name)
                logger.error(
                    f"!!! ALERTA: Los {n_sample} links de prueba para '{super_name}' fallaron. "
                    f"Se EXCLUYE dinámicamente '{super_name}' de esta corrida."
                )
            else:
                supers_activos.append(super_name)

        # Construir el texto de observación para la base de datos
        activos_count = len(supers_activos)
        if supers_caidos:
            caidos_str = ", ".join(supers_caidos)
            observaciones = f"Se ejecutaron {activos_count} de {total_supers} supermercados. Revisar: {caidos_str}"
        else:
            observaciones = f"Extracción completa: se ejecutaron {activos_count} de {total_supers} supermercados."

        links_validos = [
            item for item in links_list 
            if (item.get('supermercado') or item.get('nombre_super')) not in supers_caidos
        ]

        logger.info(f"[PRE-CHECK] Resumen: {observaciones}")
        return links_validos, supers_caidos, observaciones