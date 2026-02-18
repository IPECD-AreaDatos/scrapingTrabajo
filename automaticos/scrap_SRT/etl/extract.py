"""
EXTRACT - Módulo de extracción de datos SRT
Responsabilidad: Los archivos CSV del SRT se descargan manualmente y se colocan en files/.
Este módulo verifica que los archivos estén disponibles.
"""
import os
import logging

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')


class ExtractSRT:
    """
    Verifica que los archivos CSV del SRT estén disponibles en files/.

    Nota: Los archivos CSV del SRT se descargan manualmente desde
    https://www.srt.gob.ar/estadisticas/ y se colocan en la carpeta files/.
    """

    def extract(self) -> str:
        """
        Verifica que existan archivos CSV en files/ y retorna la ruta.

        Returns:
            str: Ruta a la carpeta files/

        Raises:
            FileNotFoundError: Si no hay archivos CSV en files/
        """
        if not os.path.exists(FILES_DIR):
            raise FileNotFoundError(f"Carpeta files/ no encontrada: {FILES_DIR}")

        csvs = [f for f in os.listdir(FILES_DIR) if f.endswith('.csv')]
        if not csvs:
            raise FileNotFoundError(
                f"No se encontraron archivos CSV en {FILES_DIR}. "
                "Descargue los archivos manualmente desde https://www.srt.gob.ar/estadisticas/"
            )

        logger.info("[EXTRACT] Encontrados %d archivos CSV en %s", len(csvs), FILES_DIR)
        return FILES_DIR
