import os
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

import shutil
from dotenv import load_dotenv
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

load_dotenv()

def wait_for_download(download_dir, timeout=60):
    """
    Waits for a file to appear in the download directory.
    """
    seconds = 0
    while seconds < timeout:
        time.sleep(1)
        # Check if any .crdownload or .tmp files exist (Chrome)
        if not glob.glob(os.path.join(download_dir, "*.crdownload")) and glob.glob(os.path.join(download_dir, "*.xls*")):
            return True
        seconds += 1
    return False

def login_and_extract():
    """
    Handles the login process and extracts the raw data.
    """
    LOGIN_URL = "https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx"
    USERNAME = os.getenv("SIIF_USERNAME", "boscof")
    PASSWORD = os.getenv("SIIF_PASSWORD", "IPECD2026")
    SLEEP_TIME = int(os.getenv("SIIF_SLEEP_BEFORE_LOGIN", "5"))
    
    print(f"Waiting {SLEEP_TIME} seconds before login...")
    time.sleep(SLEEP_TIME)
    
    print(f"Starting extraction from {LOGIN_URL}")
    
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Configure download directory
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    download_dir = os.path.join(BASE_DIR, "files/raw")
    os.makedirs(download_dir, exist_ok=True)
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    
    try:
        driver.get(LOGIN_URL)
        print(f"Current URL: {driver.current_url}")
        
        wait = WebDriverWait(driver, 30)
        
        # 1. Login
        try:
            user_field = wait.until(EC.presence_of_element_located((By.ID, "pt1:it1::content")))
            print("Login field found.")
        except Exception as e:
            print(f"Login field not found. Current URL: {driver.current_url}")
            os.makedirs(download_dir, exist_ok=True)
            driver.save_screenshot(os.path.join(download_dir, "login_error.png"))
            if "mainSiif" in driver.current_url and "login" not in driver.current_url:
                print("Already logged in?")
            else:
                print(f"Page source snippet: {driver.page_source[:500]}")
            raise e
            
        def slow_type(element, text):
            try:
                element.clear()
            except: pass
            
            # Use JS to ensure it's not blocked
            driver.execute_script("arguments[0].value = '';", element)
            element.send_keys(text)
            time.sleep(0.5)

        print(f"Loaded credentials. User length: {len(USERNAME) if USERNAME else 0}, Pass length: {len(PASSWORD) if PASSWORD else 0}")
        
        user_field = wait.until(EC.element_to_be_clickable((By.ID, "pt1:it1::content")))
        slow_type(user_field, USERNAME)
        
        pass_field = wait.until(EC.element_to_be_clickable((By.ID, "pt1:it2::content")))
        slow_type(pass_field, PASSWORD)
        
        # Try clicking the login button explicitly
        try:
            login_btn = wait.until(EC.element_to_be_clickable((By.ID, "pt1:cb1")))
            login_btn.click()
            print("Login button clicked.")
        except:
            print("Login button not found, trying ENTER on password field.")
            pass_field.send_keys(Keys.ENTER)
        
        time.sleep(5)
        
        try:
            print("Checking for active session dialog...")
            aceptar_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Aceptar')] | //a[contains(., 'Aceptar')] | //span[text()='Aceptar']"))
            )
            aceptar_btn.click()
            print("Handled active session dialog.")
            time.sleep(5)
        except:
            print("No active session dialog found or timed out.")
            pass
            
        print("Waiting for Main Menu (timeout 60s)...")
        driver.save_screenshot(os.path.join(download_dir, "pre_menu_wait.png"))
        try:
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "pt1:cb12")))
        except:
            print("Menu ID not found, trying by text...")
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'REPORTES')] | //a[contains(text(), 'REPORTES')]")))
        
        print("Successfully reached Main Menu!")

        # 2. Navigate to REPORTES -> Reportes
        try:
            wait.until(EC.element_to_be_clickable((By.ID, "pt1:cb12"))).click() # REPORTES
        except:
            wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'REPORTES')] | //a[contains(text(), 'REPORTES')]"))).click()
            
        wait.until(EC.element_to_be_clickable((By.ID, "pt1:cb14"))).click() # Reportes sub-menu
        time.sleep(2)
        
        # 3. Handle Popup
        main_window = driver.current_window_handle
        wait.until(lambda d: len(d.window_handles) > 1)
        
        for handle in driver.window_handles:
            if handle != main_window:
                driver.switch_to.window(handle)
                print(f"Switched to window: {handle} | Title: {driver.title} | URL: {driver.current_url}")
                break
        
        print("Switched to report popup.")
        time.sleep(5)
        os.makedirs(download_dir, exist_ok=True)
        driver.save_screenshot(os.path.join(download_dir, "popup_debug.png"))
        popup_window = driver.current_window_handle
        
        # 4. Select Module and Report
        for i in range(5):
            try:
                module_dropdown = wait.until(EC.presence_of_element_located((By.ID, "pt1:socModulo::content")))
                select = Select(module_dropdown)
                select.select_by_visible_text("SUB - SISTEMA DE CONTROL DE GASTOS")
                print("Module selected.")
                time.sleep(3)
                
                # Filter by name "rf604m" in the second column (c2) and press ENTER
                filter_id = "_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c2::content"
                filter_field = wait.until(EC.presence_of_element_located((By.ID, filter_id)))
                filter_field.clear()
                filter_field.send_keys("rf604m")
                filter_field.send_keys(Keys.ENTER)
                print("Filter applied with ENTER.")
                time.sleep(5)
                
                # IMPORTANT: Click the row with the text rf604m
                report_row = wait.until(EC.element_to_be_clickable((By.XPATH, "//tr[td[contains(., 'rf604m')]]")))
                report_row.click()
                print("Report row selected.")
                time.sleep(1)
                
                # Click Siguiente
                siguiente_btn = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Siguiente")))
                siguiente_btn.click()
                print("Clicked Siguiente.")
                time.sleep(5)
                driver.save_screenshot(os.path.join(download_dir, "parameters_debug.png"))
                break
            except Exception as e:
                if i == 4: 
                    driver.save_screenshot(os.path.join(download_dir, f"report_selection_error_{i}.png"))
                    raise e
                print(f"Retrying report selection (attempt {i+1}) due to: {type(e).__name__}")
                time.sleep(3)
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
        
        periods = [
            {"year": "2022", "months": [str(m).zfill(2) for m in range(1, 13)]},
            {"year": "2023", "months": [str(m).zfill(2) for m in range(1, 13)]},
            {"year": "2024", "months": [str(m).zfill(2) for m in range(1, 13)]},
            {"year": "2025", "months": [str(m).zfill(2) for m in range(1, 13)]},
            {"year": "2026", "months": [str(m).zfill(2) for m in range(1, 6)]}
        ]
        
        entidad_select_el = wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Entidad Inicial')]//following::select[1] | //select[contains(@id, 'socEntidadInicial')]")))
        entidad_select = Select(entidad_select_el)
        entities = [opt.text for opt in entidad_select.options if opt.text.strip() and opt.get_attribute("value")]
        
        log_file = os.path.join(download_dir, "scraping_log.txt")
        with open(log_file, "a") as log:
            log.write(f"\n--- Starting Scraping Session: {time.ctime()} ---\n")

        # Process all entities
        for entity in entities:
            print(f"Processing Entity: {entity}")
            
            for period in periods:
                year = period["year"]
                
                try:
                    # 1. Year
                    anio_field = wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Año')]//following::input[1] | //input[contains(@id, 'txtAnioEjercicio')]")))
                    anio_field.clear()
                    anio_field.send_keys(year)
                    time.sleep(1)
                    
                    # 2. Entity Initial/Final
                    select_ini = Select(driver.find_element(By.XPATH, "//label[contains(text(), 'Entidad Inicial')]//following::select[1] | //select[contains(@id, 'socEntidadInicial')]"))
                    select_ini.select_by_visible_text(entity)
                    time.sleep(2)
                    select_fin = Select(wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Entidad Final')]//following::select[1] | //select[contains(@id, 'socEntidadFinal')]"))))
                    select_fin.select_by_visible_text(entity)
                    
                    for month in period["months"]:
                        print(f"  - {year} Month: {month}")
                        
                        clean_entity = "".join([c for c in entity if c.isalnum() or c in (" ", "_")]).strip()
                        filename = f"{year}{month}_{clean_entity}.xls"
                        dest_folder = os.path.join(download_dir, year, month)
                        dest_path = os.path.join(dest_folder, filename)
                        if os.path.exists(dest_path):
                            print(f"  - Already downloaded: {dest_path}")
                            continue
                            
                        try:
                            # 3. Month (Unique field for rf604m)
                            mes_field = wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Mes')]//following::input[1] | //input[contains(@id, 'txtMesEjercicio')]")))
                            mes_field.clear()
                            mes_field.send_keys(month)
                            
                            # Select XLS
                            try: driver.find_element(By.XPATH, "//label[contains(text(), 'XLS')]//preceding-sibling::input[1] | //input[contains(@id, 'rbtnXLS')]").click()
                            except: pass
                            
                            # Clear old stray downloads
                            for f in glob.glob(os.path.join(download_dir, "*.xls*")):
                                try: os.remove(f)
                                except: pass
                                
                            current_handles_before = driver.window_handles
                            
                            # Generate Report
                            try:
                                generate_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Ver Reporte')] | //span[contains(text(), 'Ver Reporte')]")))
                                driver.execute_script("arguments[0].scrollIntoView();", generate_btn)
                                time.sleep(1)
                                try:
                                    generate_btn.click()
                                except:
                                    driver.execute_script("arguments[0].click();", generate_btn)
                            except:
                                generate_btn = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Ver Reporte")))
                                try:
                                    generate_btn.click()
                                except:
                                    driver.execute_script("arguments[0].click();", generate_btn)
                            
                            print("  - Clicked Ver Reporte. Waiting for action...")
                            
                            # Custom wait for either new window OR "No se encontraron" dialog
                            is_no_data = False
                            window_opened = False
                            start_wait = time.time()
                            while time.time() - start_wait < 30:
                                if len(driver.window_handles) > len(current_handles_before):
                                    window_opened = True
                                    break
                                try:
                                    # Check for error text
                                    if driver.find_elements(By.XPATH, "//*[contains(text(), 'No se encontraron') or contains(text(), 'registros')]"):
                                        is_no_data = True
                                        break
                                except: pass
                                time.sleep(0.5)
                            
                            if is_no_data:
                                print(f"  - No data found for {entity} - {year}/{month}")
                                with open(log_file, "a") as log:
                                    log.write(f"NO DATA: {entity} | {year}-{month}\n")
                                try:
                                    aceptar_error = driver.find_element(By.XPATH, "//button[contains(., 'Aceptar')] | //span[text()='Aceptar']")
                                    aceptar_error.click()
                                except: pass
                                if len(driver.window_handles) > len(current_handles_before):
                                    driver.close()
                                    driver.switch_to.window(popup_window)
                                continue
                            
                            if window_opened:
                                current_handles_after = driver.window_handles
                                driver.switch_to.window(current_handles_after[-1])
                            
                            if wait_for_download(download_dir, timeout=60):
                                files = glob.glob(os.path.join(download_dir, "*.xls*"))
                                latest_file = max(files, key=os.path.getctime)
                                dest_folder = os.path.join(download_dir, year, month)
                                os.makedirs(dest_folder, exist_ok=True)
                                clean_entity = "".join([c for c in entity if c.isalnum() or c in (" ", "_")]).strip()
                                filename = f"{year}{month}_{clean_entity}.xls"
                                dest_path = os.path.join(dest_folder, filename)
                                shutil.move(latest_file, dest_path)
                                print(f"  - Downloaded: {dest_path}")
                            else:
                                print(f"  - Download timeout for {entity} - {year}/{month}")
                                driver.save_screenshot(os.path.join(download_dir, f"timeout_{year}_{month}.png"))
                            
                            if len(driver.window_handles) > len(current_handles_before):
                                driver.close()
                                driver.switch_to.window(popup_window)
                                
                        except Exception as e:
                            print(f"  - Error processing month {month}: {e}")
                            with open(log_file, "a") as log:
                                log.write(f"ERROR_MONTH: {entity} | {year}-{month} | {str(e)[:50]}\n")
                            for h in driver.window_handles:
                                if h == popup_window:
                                    driver.switch_to.window(h)
                                    break
                            time.sleep(5)
                except Exception as e:
                    print(f"  - Error processing year {year}: {e}")
                    time.sleep(5)

    except Exception as e:
        print(f"An error occurred: {e}")
        try: driver.save_screenshot(os.path.join(download_dir, "error_screenshot.png"))
        except: pass
        raise e
    finally:
        driver.quit()

if __name__ == "__main__":
    login_and_extract()
