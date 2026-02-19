"""
Opciones de Chrome optimizadas para Linux server (headless, sin sandbox).
Uso:
    from shared.chrome_options import get_chrome_options
    driver = webdriver.Chrome(options=get_chrome_options())
"""
from selenium import webdriver


def get_chrome_options(download_dir: str = None) -> webdriver.ChromeOptions:
    """
    Retorna ChromeOptions configuradas para entorno Linux/servidor headless.

    Args:
        download_dir: Directorio de descarga automática (opcional).

    Returns:
        webdriver.ChromeOptions configurado.
    """
    options = webdriver.ChromeOptions()

    # Modo sin interfaz gráfica (obligatorio en servidor)
    options.add_argument('--headless')

    # Linux: el proceso de Chrome no puede usar el sandbox si corre como root
    options.add_argument('--no-sandbox')

    # /dev/shm suele ser pequeño en servidores; usar /tmp en su lugar
    options.add_argument('--disable-dev-shm-usage')

    # Sin GPU en servidor
    options.add_argument('--disable-gpu')

    # Ventana estándar para evitar saltos de layout
    options.add_argument('--window-size=1920,1080')

    # Velocidad: deshabilitar funciones innecesarias
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')

    # RAM: evitar proceso de GPU separado
    options.add_argument('--disable-software-rasterizer')

    # Directorio de descarga automática
    if download_dir:
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

    return options
