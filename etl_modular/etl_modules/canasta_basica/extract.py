import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def extract_products_data(links, supermercado):
    """
    Extrae datos de productos desde una lista de links usando Selenium.
    
    Args:
        links (list): Lista de URLs de productos
        supermercado (str): Nombre del supermercado
    Returns:
        pd.DataFrame: Datos extraídos
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 25)

    productos = []

    try:
        for url in links:
            print(f"\n🌐 Intentando abrir: {url}")

            try:
                driver.get(url)
                print(f"🔎 Página cargada: {driver.title}")

                html_len = len(driver.page_source)
                print(f"📄 HTML length: {html_len} caracteres")

                # Nombre del producto
                print("⏳ Buscando nombre...")
                nombre_elem = driver.find_element(By.CSS_SELECTOR, "h1 .vtex-store-components-3-x-productBrand")
                nombre = nombre_elem.text.strip()
                print(f"✅ Nombre encontrado: {nombre}")

                # Precio del producto
                print("⏳ Buscando precio...")
                precio_elem = WebDriverWait(driver, 10).until(     
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.valtech-carrefourar-product-price-0-x-currencyContainer"))
                )
                precio = precio_elem.text.strip()
                print(f"✅ Precio encontrado: {precio}")

                # Intentar encontrar EAN
                print("⏳ Buscando EAN...")
                try:
                    ean_elem = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "//td[@data-specification='EAN']/div"))
                    )
                    ean = ean_elem.text.strip()
                    print(f"✅ EAN encontrado: {ean}")
                except:
                    ean = None
                    print("⚠️ No se encontró EAN")

                # Agregar a la lista
                productos.append({
                    "nombre": nombre,
                    "precio": precio,
                    "fecha": datetime.today().strftime("%Y-%m-%d"),
                    "supermercado": supermercado,
                    "codigo_ean": ean,
                    "url": url
                })

            except Exception as e:
                print(f"❌ Error procesando {url}: {e}")
                # Guardar HTML para inspección
                with open("error_page.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print("💾 HTML guardado como error_page.html")

        return pd.DataFrame(productos)

    finally:
        driver.quit()