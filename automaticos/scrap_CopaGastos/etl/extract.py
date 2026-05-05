import os
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

import shutil

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
    USERNAME = "boscof"
    PASSWORD = "IPECD2026"
    
    print("Waiting 2 minutes before login to avoid session conflicts...")
    time.sleep(120)
    
    print(f"Starting extraction from {LOGIN_URL}")
    
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Configure download directory
    download_dir = os.path.abspath("files/raw")
    os.makedirs(download_dir, exist_ok=True)
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    
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
            # Check if we are already logged in or in an error page
            if "mainSiif" in driver.current_url and "login" not in driver.current_url:
                print("Already logged in?")
            else:
                print(f"Page source snippet: {driver.page_source[:500]}")
            raise e
            
        # Helper for slow typing
        def slow_type(element, text):
            element.click()
            element.send_keys(Keys.CONTROL + "a")
            element.send_keys(Keys.BACKSPACE)
            for char in text:
                element.send_keys(char)
                time.sleep(0.1)

        from selenium.webdriver.common.keys import Keys
        slow_type(user_field, USERNAME)
        
        pass_field = driver.find_element(By.ID, "pt1:it2::content")
        slow_type(pass_field, PASSWORD)
        
        # Try both clicking and pressing Enter
        pass_field.send_keys(Keys.ENTER)
        time.sleep(5)
        
        try:
            login_btn = driver.find_element(By.ID, "pt1:cb1")
            login_btn.click()
            print("Login button clicked.")
        except:
            print("Login button not found or already clicked via ENTER.")
        
        # Handle potential "Session already active" dialog
        try:
            print("Checking for active session dialog...")
            # Wait for any dialog that might have an Aceptar button
            aceptar_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Aceptar')] | //a[contains(., 'Aceptar')] | //span[text()='Aceptar']"))
            )
            aceptar_btn.click()
            print("Handled active session dialog.")
            time.sleep(5) # Wait for page to transition after clicking Aceptar
        except:
            print("No active session dialog found or timed out.")
            pass
            
        # Wait for menu to load
        wait.until(EC.presence_of_element_located((By.ID, "pt1:cb12")))
        print("Successfully reached Main Menu!")

        # 2. Navigate to REPORTES -> Reportes
        wait.until(EC.element_to_be_clickable((By.ID, "pt1:cb12"))).click() # REPORTES
        wait.until(EC.element_to_be_clickable((By.ID, "pt1:cb14"))).click() # Reportes sub-menu
        
        # 3. Handle Popup
        main_window = driver.current_window_handle
        wait.until(lambda d: len(d.window_handles) > 1)
        
        for handle in driver.window_handles:
            if handle != main_window:
                driver.switch_to.window(handle)
                break
        
        print("Switched to report popup.")
        time.sleep(5) # Wait for popup to settle
        popup_window = driver.current_window_handle
        
        # 4. Select Module and Report
        for i in range(5):
            try:
                module_dropdown = wait.until(EC.presence_of_element_located((By.ID, "pt1:socModulo::content")))
                select = Select(module_dropdown)
                select.select_by_visible_text("SUB - SISTEMA DE CONTROL DE GASTOS")
                print("Module selected.")
                time.sleep(3) # Wait for table to update after module selection
                
                # Filter by code 2336 with retry
                filter_id = "_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content"
                filter_field = wait.until(EC.presence_of_element_located((By.ID, filter_id)))
                filter_field.clear()
                filter_field.send_keys("2336")
                print("Filter applied.")
                time.sleep(5) # Wait for table to refresh after filter
                
                # IMPORTANT: Click the row with the code 2336 to ensure it's selected
                report_row = wait.until(EC.element_to_be_clickable((By.XPATH, "//tr[td[contains(., '2336')]]")))
                report_row.click()
                print("Report row selected.")
                time.sleep(1)
                
                # Click Siguiente
                siguiente_btn = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Siguiente")))
                siguiente_btn.click()
                print("Clicked Siguiente.")
                break
            except Exception as e:
                if i == 4: raise e
                print(f"Retrying report selection (attempt {i+1}) due to: {type(e).__name__}")
                time.sleep(3)
                driver.switch_to.window(driver.window_handles[-1])
        
        # 5. Set Parameters and Download (Loop through entities and months)
        wait.until(EC.presence_of_element_located((By.ID, "pt1:txtAnioEjercicio::content"))).clear()
        driver.find_element(By.ID, "pt1:txtAnioEjercicio::content").send_keys("2025")
        
        # Select XLS
        wait.until(EC.element_to_be_clickable((By.ID, "pt1:rbtnXLS::content"))).click()
        print("XLS selected.")
        
        print("Ready for parameter loop.")
        
        # 6. Extraction Loop
        periods = [
            {"year": "2025", "months": [str(m).zfill(2) for m in range(1, 13)]},
            {"year": "2026", "months": [str(m).zfill(2) for m in range(1, 6)]}
        ]
        
        entidad_select_el = wait.until(EC.presence_of_element_located((By.ID, "pt1:socEntidadInicial::content")))
        entidad_select = Select(entidad_select_el)
        entities = [opt.text for opt in entidad_select.options if opt.text.strip() and opt.get_attribute("value")]
        
        log_file = "files/raw/scraping_log.txt"
        with open(log_file, "a") as log:
            log.write(f"\n--- Starting Scraping Session: {time.ctime()} ---\n")

        for entity in entities:
            print(f"Processing Entity: {entity}")
            
            for period in periods:
                year = period["year"]
                
                try:
                    year_field = wait.until(EC.presence_of_element_located((By.ID, "pt1:txtAnioEjercicio::content")))
                    year_field.clear()
                    year_field.send_keys(year)
                    time.sleep(1)
                    
                    select_ini = Select(driver.find_element(By.ID, "pt1:socEntidadInicial::content"))
                    select_ini.select_by_visible_text(entity)
                    
                    time.sleep(2)
                    select_fin = Select(wait.until(EC.presence_of_element_located((By.ID, "pt1:socEntidadFinal::content"))))
                    select_fin.select_by_visible_text(entity)
                    
                    for month in period["months"]:
                        print(f"  - {year} Month: {month}")
                        
                        try:
                            mes_desde = wait.until(EC.presence_of_element_located((By.ID, "pt1:txtMesDesde::content")))
                            mes_desde.clear()
                            mes_desde.send_keys(month)
                            
                            mes_hasta = driver.find_element(By.ID, "pt1:txtMesHasta::content")
                            mes_hasta.clear()
                            mes_hasta.send_keys(month)
                            
                            try: driver.find_element(By.ID, "pt1:rbtnXLS::content").click()
                            except: pass
                            
                            current_handles_before = driver.window_handles
                            
                            # Generate Report
                            try:
                                user_xpath = "/html/body/div[1]/form/div[1]/div[5]/div/div[1]/div[2]/div/div[3]/div/div[4]/div/div[1]/div[1]/table/tbody/tr/td[5]/div/a/span"
                                generate_btn = wait.until(EC.element_to_be_clickable((By.XPATH, user_xpath)))
                                driver.execute_script("arguments[0].scrollIntoView();", generate_btn)
                                time.sleep(1)
                                generate_btn.click()
                            except:
                                generate_btn = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Ver Reporte")))
                                generate_btn.click()
                            
                            print("  - Clicked Ver Reporte. Waiting for window or download...")
                            time.sleep(5)
                            
                            # Check for new window
                            current_handles_after = driver.window_handles
                            if len(current_handles_after) > len(current_handles_before):
                                print("  - New window detected. Switching...")
                                driver.switch_to.window(current_handles_after[-1])
                                time.sleep(5)
                            
                            # Check for "No se encontraron datos"
                            try:
                                error_dialog = WebDriverWait(driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'No se encontraron') or contains(text(), 'registros')]"))
                                )
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
                            except:
                                pass
                            
                            # Wait for download (60s)
                            if wait_for_download(download_dir, timeout=60):
                                files = glob.glob(os.path.join(download_dir, "*.xls*"))
                                latest_file = max(files, key=os.path.getctime)
                                
                                dest_folder = os.path.join(download_dir, year, month)
                                os.makedirs(dest_folder, exist_ok=True)
                                
                                clean_entity = "".join([c for c in entity if c.isalnum() or c in (" ", "_")]).strip()
                                dest_path = os.path.join(dest_folder, f"{clean_entity}.xls")
                                
                                shutil.move(latest_file, dest_path)
                                print(f"  - Downloaded: {dest_path}")
                            else:
                                print(f"  - Download timeout for {entity} - {year}/{month}")
                                driver.save_screenshot(f"files/raw/timeout_{year}_{month}.png")
                                with open(log_file, "a") as log:
                                    log.write(f"TIMEOUT: {entity} | {year}-{month}\n")
                            
                            if len(driver.window_handles) > len(current_handles_before):
                                driver.close()
                                driver.switch_to.window(popup_window)
                                
                        except Exception as e:
                            print(f"  - Error processing month {month}: {e}")
                            with open(log_file, "a") as log:
                                log.write(f"ERROR_MONTH: {entity} | {year}-{month} | {str(e)[:50]}\n")
                            # Try to stay in popup
                            for h in driver.window_handles:
                                if h == popup_window:
                                    driver.switch_to.window(h)
                                    break
                            time.sleep(5)
                except Exception as e:
                    print(f"  - Error processing year {year}: {e}")
                    with open(log_file, "a") as log:
                        log.write(f"ERROR_YEAR: {entity} | {year} | {str(e)[:50]}\n")
                    time.sleep(5)

    except Exception as e:
        print(f"An error occurred: {e}")
        driver.save_screenshot("files/raw/error_screenshot.png")
        raise e
    finally:
        driver.quit()

if __name__ == "__main__":
    login_and_extract()
