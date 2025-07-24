import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# Funciones de utilidad para el navegador
def switch_to_latest_window(driver):
    windows = driver.window_handles
    driver.switch_to.window(windows[-1])

def close_current_window(driver, original_window):
    driver.close()
    driver.switch_to.window(original_window)

# --- FUNCIÓN PRINCIPAL DE EXTRACCIÓN ---
def extract_dnrpa_data(mode='last'):
    """
    Extrae los datos brutos de DNRPA.

    Args:
        mode (str): 'last' para el último año, 'historical' para todos los años.
    """
    print(f"📥 Iniciando extracción de datos de DNRPA en modo '{mode}'...")

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    original_window = driver.current_window_handle
    
    driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

    # Almacenará los datos brutos (listas de listas o HTML)
    raw_data = []

    # Bucle principal de extracción
    try:
        # Lógica para seleccionar los años
        elemento = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select'))
        )
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        
        anos_a_procesar = []
        if mode == 'last':
            # Lógica para obtener el último año
            opciones_validas = [opcion for opcion in opciones if opcion.text and opcion.text.isdigit()]
            if opciones_validas:
                ultimo_anio = max([int(opcion.text) for opcion in opciones_validas])
                anos_a_procesar.append(str(ultimo_anio))
        else: # modo 'historical'
            # Lógica para obtener todos los años
            opciones.reverse() # para ir del más reciente al más antiguo
            for opcion in opciones:
                if opcion.get_attribute('value') != '' and int(opcion.get_attribute('value')) >= 2014:
                    anos_a_procesar.append(opcion.get_attribute('value'))

        # Bucle principal de extracción por año y tipo de vehículo
        for anio in anos_a_procesar:
            for tipo_vehiculo in [1, 2]:  # 1 para Autos, 2 para Motos
                
                # Seleccionar año y tipo de vehículo
                driver.find_element(By.XPATH, f'//select/option[@value="{anio}"]').click()
                radio_xpath = '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[1]' if tipo_vehiculo == 1 else '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]'
                driver.find_element(By.XPATH, radio_xpath).click()
                
                # Click en el botón de aceptar
                driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/center/input').click()
                
                switch_to_latest_window(driver)
                
                # Extraer los datos brutos de la tabla
                tabla = driver.find_element(By.XPATH, '//*[@id="seleccion"]/div/table')
                filas = tabla.find_elements(By.TAG_NAME, "tr")
                datos_tabla = []
                for fila in filas:
                    celdas = fila.find_elements(By.TAG_NAME, "td")
                    datos_tabla.append([celda.text for celda in celdas])
                
                raw_data.append({
                    'year': anio,
                    'tipo_vehiculo': tipo_vehiculo,
                    'table_data': datos_tabla
                })
                
                close_current_window(driver, original_window)

    except Exception as e:
        print(f"❌ Error durante la extracción: {e}")
        raw_data = None # Devuelve None si hay un error
    finally:
        driver.quit() # Asegúrate de cerrar el driver al final

    print("✅ Extracción de datos de DNRPA finalizada.")
    return raw_data