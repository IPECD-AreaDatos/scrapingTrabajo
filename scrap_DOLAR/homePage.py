#Clase encargada de descargar
class HomePage:   
    """
    Con esta funcion buscamos obtener los datos de multiples dolares
    desde el sitio oficial de Ambito, es posible reutilizar esta funcion
    desde un main.py pasandole como parametro el valor de un atributo
    de la instancia.
    """
    def dolar_blue_ccl_mep(self,url):

        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web y esperamos un momento para que aparezcan las publicidades 
        self.driver.get(url)
        time.sleep(10)
        
        wait = WebDriverWait(self.driver, 10)
        # Verificar si hay iframes presentes
        iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
        
        popup = wait.until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button")))
        self.driver.execute_script("arguments[0].scrollIntoView();", popup)
        popup.click()
        
        if iframes:
            self.driver.switch_to.default_content()

            wait = WebDriverWait(self.driver, 30)
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")))
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")))

            # Encontrar elementos de fecha desde y fecha hasta
            fecha_desde = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
            fecha_hasta = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")

            # Fecha actual y cadena del día anterior
            fecha_actual = datetime.now()

            # Cadena del día anterior
            dia_anterior = str((fecha_actual.day) - 1) + "/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

            # Fechas de inicio y fin
            fecha_desde.clear()
            fecha_desde.send_keys("01/01/2003")
            fecha_hasta.clear()
            fecha_hasta.send_keys(dia_anterior)
            
            # Esperar hasta que el botón sea clickeable
            boton = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__button.boton")))
            # Desplazarse hasta el elemento
            ActionChains(self.driver).move_to_element(boton).perform()
            boton.click()
            time.sleep(20)
            
        else:
            wait = WebDriverWait(self.driver, 20)
            #wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker datepicker desde form-control")))
            #wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker datepicker hasta form-control")))
            
            fecha_desde = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
            fecha_hasta = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")
            
            #Fecha actual y la cadena del dia anterior
            fecha_actual = datetime.now()

            #Cadena del dia anterior
            dia_anterior = str((fecha_actual.day)- 1 ) +"/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

            #Fechas de inicio y fin
            fecha_desde.clear() 
            fecha_desde.send_keys("01/01/2003")
            fecha_hasta.clear()
            fecha_hasta.send_keys(dia_anterior)
            
            wait = WebDriverWait(self.driver, 20)
            #Obtener boton
            boton = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__button.boton")))
            # Desplazarse hasta el elemento
            ActionChains(self.driver).move_to_element(boton).perform()
            boton.click()
            time.sleep(20)
            
        table = self.driver.find_element(By.CLASS_NAME, 'general-historical__table')

        # Obtener el HTML de la tabla
        table_html = table.get_attribute('outerHTML')

        # Reemplazar comas por puntos en los datos
        table_html = table_html.replace(',', '.')

        # Leer la tabla HTML en un DataFrame de pandas
        df = pd.read_html(table_html)[0]
        print(df)
        
instancia = HomePage()
#instancia.dolar_blue_ccl_mep('https://www.ambito.com/contenidos/dolar-informal-historico.html')
#instancia.dolar_blue_ccl_mep('https://www.ambito.com/contenidos/dolar-mep-historico.html')
instancia.dolar_blue_ccl_mep('https://www.ambito.com/contenidos/dolar-cl-historico.html')



