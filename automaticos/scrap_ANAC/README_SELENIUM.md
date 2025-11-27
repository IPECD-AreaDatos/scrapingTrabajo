# 🔧 Configuración Selenium para Linux (Headless)

## 📝 Nota Importante

**El scraper ANAC actualmente NO usa Selenium**, utiliza `requests` que funciona perfectamente en Linux sin configuración adicional.

## 🐧 Si necesitas usar Selenium en Linux (headless)

### Instalación de dependencias:

```bash
# Instalar Chrome/Chromium
sudo apt-get update
sudo apt-get install -y chromium-browser chromium-chromedriver

# O instalar Chrome desde Google
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f -y
```

### Configuración en el código:

El `extract.py` ya tiene soporte para Selenium con headless:

```python
# Usar Selenium con headless (para Linux)
extractor = ExtractANAC(use_selenium=True, headless=True)
file_path = extractor.extract()
```

### Opciones de Chrome para Linux:

- `--headless`: Ejecuta sin ventana (necesario en servidores)
- `--no-sandbox`: Necesario para ejecutar como root o en contenedores
- `--disable-dev-shm-usage`: Evita problemas de memoria compartida
- `--disable-gpu`: No necesita GPU en servidor

## ✅ Recomendación

**Usa `requests` (por defecto)** - Es más rápido, más simple y funciona perfectamente en Linux sin configuración adicional.

Solo usa Selenium si:
- La descarga requiere JavaScript
- Hay autenticación compleja
- Necesitas interactuar con elementos dinámicos


