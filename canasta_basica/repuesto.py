def guardar_sesion(self):
        """Guarda la sesión - SOLO UNA VEZ"""
        if self.driver and self.sesion_iniciada:
            try:
                cookies = self.driver.get_cookies()
                with open(self.cookies_file, 'wb') as f:
                    pickle.dump(cookies, f)
                logger.info("💾 Sesión guardada")
                return True
            except Exception as e:
                logger.error(f"Error guardando sesión: {e}")
        return False

    def cargar_sesion(self):
        """Carga una sesión previamente guardada"""
        if not os.path.exists(self.cookies_file):
            return False
            
        try:
            # Cargar cookies
            with open(self.cookies_file, 'rb') as f:
                cookies = pickle.load(f)
            
            # Cargar localStorage
            local_storage = {}
            if os.path.exists(self.local_storage_file):
                with open(self.local_storage_file, 'rb') as f:
                    local_storage = pickle.load(f)
            
            # Ir a la página principal primero
            self.driver.get("https://www.carrefour.com.ar")
            
            # Limpiar cookies existentes y agregar las guardadas
            self.driver.delete_all_cookies()
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    logger.debug(f"Cookie no pudo ser añadida: {e}")
            
            # Restaurar localStorage
            for key, value in local_storage.items():
                self.driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")
            
            # Refrescar la página para aplicar la sesión
            self.driver.refresh()
            time.sleep(3)
            
            # Verificar si la sesión sigue activa
            if self.verificar_sesion_activa():
                self.sesion_iniciada = True
                logger.info("✅ Sesión cargada exitosamente")
                return True
            else:
                logger.warning("⚠️ Sesión cargada pero expirada")
                return False
                
        except Exception as e:
            logger.error(f"Error cargando sesión: {e}")
            return False

    def verificar_sesion_activa(self):
        """Verifica si la sesión está activa"""
        try:
            # Ir a página principal
            self.driver.get("https://www.carrefour.com.ar")
            time.sleep(2)
            
            # Buscar indicadores de usuario logueado
            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]",
                "//*[contains(text(), 'Cuenta')]"
            ]
            
            for indicador in indicadores:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        logger.info("✅ Sesión activa verificada")
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Error verificando sesión: {e}")
            return False

    def hacer_clic_ingresar(self):
        """Hace clic en el botón Ingresar de la página principal"""
        try:
            logger.info("Buscando botón 'Ingresar'...")
            
            # ESTRATEGIA 1: Ir directamente a la página de login
            try:
                self.driver.get("https://www.carrefour.com.ar/login")
                time.sleep(3)
                logger.info("✅ Navegado directamente a página de login")
                return True
            except Exception as e:
                logger.debug(f"No se pudo navegar directamente a login: {e}")
            
            # ESTRATEGIA 2: XPath exacto que me pasaste
            xpath_ingresar = "/html/body/div[3]/div/div[1]/div/div[4]/div[1]/div/div/div[3]/section/div/div[3]"
            
            try:
                boton = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath_ingresar))
                )
                if boton.is_displayed():
                    self.driver.execute_script("arguments[0].style.border='3px solid red';", boton)
                    self.driver.execute_script("arguments[0].click();", boton)
                    logger.info("✅ Clic en Ingresar (XPath específico)")
                    time.sleep(3)
                    return True
            except Exception as e:
                logger.debug(f"XPath específico falló: {e}")
            
            # ESTRATEGIA 3: Buscar en elementos más específicos
            selectores_avanzados = [
                "//div[@class='vtex-menu-2-x-styledLinkContent vtex-menu-2-x-styledLinkContent--login']",
                "//span[contains(text(), 'Ingresar')]/parent::button",
                "//div[contains(@class, 'login')]//button",
                "//button[contains(@class, 'login-button')]"
            ]
            
            for selector in selectores_avanzados:
                try:
                    boton = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    if boton.is_displayed():
                        self.driver.execute_script("arguments[0].style.border='3px solid blue';", boton)
                        self.driver.execute_script("arguments[0].click();", boton)
                        logger.info(f"✅ Clic en Ingresar (selector avanzado: {selector})")
                        time.sleep(3)
                        return True
                except Exception as e:
                    logger.debug(f"Selector avanzado falló: {selector} - {e}")
                    continue
            
            logger.error("❌ No se pudo encontrar botón Ingresar")
            return False
                
        except Exception as e:
            logger.error(f"Error haciendo clic en Ingresar: {e}")
            return False
        
    def esperar_modal_login(self):
        """Espera a que el modal de login se cargue correctamente"""
        try:
            logger.info("Esperando a que cargue el modal de login...")
            
            # Esperar indicadores de que el modal está cargado
            indicadores_modal = [
                "//div[contains(@class, 'login-modal')]",
                "//div[contains(@class, 'vtex-login')]",
                "//div[contains(@class, 'modal')]",
                "//h2[contains(text(), 'Ingresar')]",
                "//h2[contains(text(), 'Login')]",
                "//input[@type='email']",
                "//input[@type='password']"
            ]
            
            for indicador in indicadores_modal:
                try:
                    elemento = self.wait.until(
                        EC.presence_of_element_located((By.XPATH, indicador))
                    )
                    if elemento.is_displayed():
                        logger.info(f"✅ Modal de login detectado: {indicador}")
                        return True
                except:
                    continue
            
            # Si no encontramos el modal, tomar screenshot para debugging
            self.driver.save_screenshot('modal_no_encontrado.png')
            logger.warning("⚠️ No se detectaron indicadores claros del modal de login")
            return False
            
        except Exception as e:
            logger.error(f"Error esperando modal de login: {e}")
            return False
        
    def verificar_formulario_login(self):
        """Verifica si ya estamos en el formulario de login con campos de email/password"""
        try:
            # Buscar campos de email y password
            campos_email = self.driver.find_elements(By.CSS_SELECTOR, "input[type='email']")
            campos_password = self.driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
            
            if campos_email and campos_password:
                logger.info("✅ Formulario de login detectado (campos email/password visibles)")
                return True
            return False
        except:
            return False
        
    
        
    

    

    def ingresar_credenciales(self):
        try:
            time.sleep(3)
            # Email
            campo_email = self.driver.find_element(By.CSS_SELECTOR, "input[type='email']")
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("✅ Email ingresado")
            time.sleep(1)
            # Contraseña
            campo_password = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("✅ Contraseña ingresada")
            time.sleep(1)
            # Login
            boton_login = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            boton_login.click()
            logger.info("✅ Clic en login")
            time.sleep(5)
            return True
        except Exception as e:
            logger.error(f"Error credenciales: {e}")
            return False


    def hacer_clic_ingresar_con_mail(self):
        """Hacer clic con debugging"""
        try:
            logger.info("Buscando botón email...")
            time.sleep(3)
            
            # Estrategia principal: buscar en modal
            try:
                modal = self.driver.find_element(By.XPATH, "//div[contains(@class, 'vtex-login')]")
                botones = modal.find_elements(By.TAG_NAME, "button")
                
                logger.info(f"Botones encontrados: {len(botones)}")
                for i, boton in enumerate(botones):
                    texto = boton.text.strip().lower()
                    logger.info(f"Botón {i}: '{texto}'")
                    if 'mail' in texto or 'email' in texto:
                        boton.click()
                        logger.info(f"✅ Clic en botón: '{texto}'")
                        time.sleep(3)
                        return True
            except Exception as e:
                logger.debug(f"Modal falló: {e}")
            
            # Estrategia secundaria
            try:
                boton = self.driver.find_element(By.XPATH, "//button[contains(., 'mail')]")
                boton.click()
                logger.info("✅ Clic en botón mail (XPath)")
                time.sleep(3)
                return True
            except:
                pass
            
            logger.error("❌ No se pudo encontrar botón email")
            return False
            
        except Exception as e:
            logger.error(f"Error en clic email: {e}")
            return False
        
    def verificar_sesion_rapida(self):
        """Verificación más rápida de sesión"""
        try:
            # Verificación simple y rápida
            elementos_indicadores = self.driver.find_elements(By.XPATH, 
                "//*[contains(text(), 'Mi cuenta') or contains(text(), 'Hola')]")
            return any(elem.is_displayed() for elem in elementos_indicadores)
        except:
            return False

   

    
        
    def verificar_formulario_login(self):
        """Verifica si ya estamos en el formulario de login con campos de email/password"""
        try:
            # Buscar campos de email y password
            campos_email = self.driver.find_elements(By.CSS_SELECTOR, "input[type='email']")
            campos_password = self.driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
            
            if campos_email and campos_password:
                logger.info("Formulario de login detectado (campos email/password visibles)")
                return True
            return False
        except:
            return False

    def cerrar(self):
        """Cierre optimizado"""
        if self.driver:
            self.guardar_sesion()  # Solo una vez
            self.driver.quit()
            self.driver = None
            self.sesion_iniciada = False
    




    def forzar_nuevo_login_y_ubicacion(self):
        """Fuerza nuevo login y configura Corrientes"""
        try:
            logger.info("=== NUEVO LOGIN CON CORRIENTES ===")
            
            # Eliminar sesión anterior
            if os.path.exists(self.cookies_file):
                os.remove(self.cookies_file)
                logger.info("🗑️  Sesión anterior eliminada")
            
            # Configurar driver
            if self.driver is None:
                self.setup_driver()
            
            # Login
            if not self.login_completo():
                return False
            
            # Configurar ubicación
            if not self.configurar_ubicacion_corrientes():
                logger.warning("⚠️  No se pudo configurar ubicación, continuando...")
            
            self.sesion_iniciada = True
            logger.info("✅ Login y configuración completados")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en forzar_nuevo_login: {e}")
            return False

    def login_completo(self):
        """Login completo - VERSIÓN SIMPLE Y FUNCIONAL"""
        try:
            logger.info("=== LOGIN SIMPLE ===")
            
            # Paso 1: Ir directamente a la página de login
            self.driver.get("https://www.carrefour.com.ar/login")
            time.sleep(4)
            
            # TOMAR SCREENSHOT INICIAL
            self.driver.save_screenshot('01_pagina_login.png')
            logger.info("📸 01_pagina_login.png")
            
            # Paso 2: Hacer clic en el botón de email de manera DIRECTA
            logger.info("🔍 Buscando botón email...")
            time.sleep(3)
            
            # ESTRATEGIA DIRECTA: Buscar por texto exacto
            try:
                boton_email = self.driver.find_element(By.XPATH, "//button[contains(., 'mail') or contains(., 'email')]")
                boton_email.click()
                logger.info("✅ Clic en botón email")
                time.sleep(3)
            except:
                logger.error("❌ No se pudo hacer clic en botón email")
                return False
            
            # TOMAR SCREENSHOT DESPUÉS DEL CLIC
            self.driver.save_screenshot('02_despues_clic_email.png')
            logger.info("📸 02_despues_clic_email.png")
            
            # Paso 3: ESPERAR A QUE CARGUE EL FORMULARIO y luego ingresar credenciales
            logger.info("⏳ Esperando formulario...")
            time.sleep(5)  # Más tiempo para que cargue
            
            # TOMAR SCREENSHOT DEL FORMULARIO
            self.driver.save_screenshot('03_formulario_cargado.png')
            logger.info("📸 03_formulario_cargado.png")
            
            # Paso 4: INGRESAR CREDENCIALES con múltiples estrategias
            if not self.ingresar_credenciales_simple():
                return False
            
            # TOMAR SCREENSHOT DESPUÉS DE CREDENCIALES
            self.driver.save_screenshot('04_despues_credenciales.png')
            logger.info("📸 04_despues_credenciales.png")
            
            # Paso 5: Verificar login
            logger.info("🔍 Verificando login...")
            time.sleep(5)
            
            if self.verificar_login_simple():
                logger.info("✅ LOGIN EXITOSO")
                return True
            else:
                logger.error("❌ LOGIN FALLIDO")
                self.driver.save_screenshot('05_login_fallido.png')
                return False
            
        except Exception as e:
            logger.error(f"❌ Error en login: {e}")
            self.driver.save_screenshot('06_error_general.png')
            return False
        
    def verificar_login_simple(self):
        """Verificación simple de login"""
        try:
            # Verificar si estamos en página principal (no en login)
            current_url = self.driver.current_url
            if 'login' not in current_url.lower():
                logger.info("✅ Login exitoso - No estamos en página de login")
                return True
            
            # Buscar indicadores de usuario logueado
            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]",
                "//*[contains(text(), 'Cuenta')]"
            ]
            
            for indicador in indicadores:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        logger.info(f"✅ Login exitoso - {elemento.text}")
                        return True
                except:
                    continue
            
            logger.error("❌ Login no exitoso")
            return False
            
        except Exception as e:
            logger.error(f"Error verificando login: {e}")
            return False
        
    def ingresar_credenciales_simple(self):
        """Ingresa credenciales de manera SIMPLE Y DIRECTA"""
        try:
            logger.info("Ingresando credenciales...")
            
            # ESTRATEGIA 1: Buscar campos con espera explícita
            try:
                # Esperar explícitamente a que aparezcan los campos
                campo_email = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'], input[name='email']"))
                )
                campo_password = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password'], input[name='password']"))
                )
                
                logger.info("✅ Campos encontrados con espera explícita")
                
            except:
                # ESTRATEGIA 2: Buscar todos los inputs y encontrar los correctos
                logger.info("Buscando todos los inputs...")
                inputs = self.driver.find_elements(By.TAG_NAME, "input")
                campo_email = None
                campo_password = None
                
                for inp in inputs:
                    try:
                        tipo = inp.get_attribute('type')
                        name = inp.get_attribute('name')
                        
                        if tipo == 'email' or 'email' in str(name).lower():
                            campo_email = inp
                            logger.info(f"✅ Campo email encontrado: type={tipo}, name={name}")
                        elif tipo == 'password' or 'password' in str(name).lower():
                            campo_password = inp
                            logger.info(f"✅ Campo password encontrado: type={tipo}, name={name}")
                    except:
                        continue
                
                if not campo_email or not campo_password:
                    logger.error("❌ No se pudieron encontrar ambos campos")
                    return False
            
            # INGRESAR EMAIL
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("✅ Email ingresado")
            time.sleep(1)
            
            # INGRESAR CONTRASEÑA
            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("✅ Contraseña ingresada")
            time.sleep(1)
            
            # BUSCAR Y HACER CLIC EN BOTÓN DE LOGIN
            boton_login = None
            selectores_boton = [
                "button[type='submit']",
                "//button[contains(., 'INICIAR SESIÓN')]",
                "//button[contains(., 'Iniciar sesión')]",
                "button.vtex-button",
                ".vtex-login-2-x-signInButton"
            ]
            
            for selector in selectores_boton:
                try:
                    if selector.startswith("//"):
                        boton_login = self.driver.find_element(By.XPATH, selector)
                    else:
                        boton_login = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if boton_login.is_displayed() and boton_login.is_enabled():
                        boton_login.click()
                        logger.info(f"✅ Clic en botón login: {selector}")
                        break
                except:
                    continue
            
            if not boton_login:
                logger.error("❌ No se pudo encontrar botón login")
                return False
            
            time.sleep(5)
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en credenciales: {e}")
            return False
        
    def ingresar_credenciales_robusto(self):
        """Ingresa credenciales de manera robusta"""
        try:
            logger.info("Ingresando credenciales...")
            time.sleep(3)
            
            # CAMPO EMAIL
            campo_email = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email']"))
            )
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("✅ Email ingresado")
            time.sleep(1)
            
            # CAMPO CONTRASEÑA
            campo_password = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
            )
            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("✅ Contraseña ingresada")
            time.sleep(1)
            
            # BOTÓN LOGIN
            boton_login = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
            )
            boton_login.click()
            logger.info("✅ Clic en botón login")
            time.sleep(5)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en credenciales: {e}")
            return False

    def configurar_ubicacion_corrientes(self):
        """Configura ubicación a Corrientes - VERSIÓN SUPER SIMPLE"""
        try:
            logger.info("📍 Configurando Corrientes...")
            
            # Ir a página principal
            self.driver.get("https://www.carrefour.com.ar")
            time.sleep(3)
            
            # Buscar selector de ubicación
            try:
                # Buscar cualquier botón que tenga que ver con ubicación
                botones_ubicacion = self.driver.find_elements(By.XPATH,
                    "//button[contains(., 'Envío') or contains(., 'Retiro') or contains(., 'ubicación')]")
                
                for boton in botones_ubicacion:
                    if boton.is_displayed():
                        boton.click()
                        logger.info("✅ Clic en selector ubicación")
                        time.sleep(2)
                        break
            except:
                logger.info("ℹ️  No se encontró selector de ubicación")
            
            # Buscar campo de código postal
            try:
                campo_cp = self.driver.find_element(By.CSS_SELECTOR,
                    "input[placeholder*='código postal'], input[placeholder*='CP'], input[name*='zip']")
                campo_cp.clear()
                campo_cp.send_keys("3400")
                logger.info("✅ Código postal 3400 ingresado")
                time.sleep(2)
                
                # Buscar botón confirmar
                try:
                    botones_confirmar = self.driver.find_elements(By.XPATH,
                        "//button[contains(., 'Confirmar') or contains(., 'Aplicar') or contains(., 'Buscar')]")
                    for boton in botones_confirmar:
                        if boton.is_displayed():
                            boton.click()
                            logger.info("✅ Clic en confirmar ubicación")
                            time.sleep(3)
                            break
                except:
                    logger.info("ℹ️  No se encontró botón confirmar")
            except:
                logger.info("ℹ️  No se encontró campo código postal")
            
            logger.info("📍 Configuración de ubicación completada")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en ubicación: {e}")
            return False

    def verificar_ubicacion_corrientes(self):
        """Verifica que la ubicación esté configurada en Corrientes"""
        try:
            logger.info("Verificando ubicación actual...")
            
            # Ir a página principal
            self.driver.get("https://www.carrefour.com.ar")
            time.sleep(3)
            
            # Buscar indicadores de ubicación Corrientes
            indicadores_corrientes = [
                "//*[contains(text(), 'Corrientes')]",
                "//*[contains(text(), 'CORRIENTES')]",
                "//*[contains(text(), '3400')]",  # Código postal
                "//*[contains(text(), 'Hiper Corrientes')]",
                "//*[contains(text(), 'Hipermercado Corrientes')]"
            ]
            
            for indicador in indicadores_corrientes:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        logger.info(f"✅ Ubicación Corrientes confirmada: {elemento.text}")
                        return True
                except:
                    continue
            
            # Verificar en el selector de ubicación
            try:
                boton_ubicacion = self.driver.find_element(By.XPATH, 
                    "//button[contains(., 'Envío') or contains(., 'Retiro')]")
                texto_ubicacion = boton_ubicacion.text
                if 'Corrientes' in texto_ubicacion or '3400' in texto_ubicacion:
                    logger.info(f"✅ Ubicación en botón: {texto_ubicacion}")
                    return True
            except:
                pass
            
            logger.warning("⚠️ No se pudo confirmar ubicación Corrientes")
            return True  # Continuamos aunque no podamos confirmar
            
        except Exception as e:
            logger.error(f"Error verificando ubicación: {e}")
            return True  # Continuamos aunque falle la verificación

    def verificar_login_exitoso(self):
        """Verifica login exitoso"""
        try:
            # Buscar indicadores simples
            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]",
                "//*[contains(text(), 'Cuenta')]"
            ]
            
            for indicador in indicadores:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        logger.info(f"✅ Login exitoso: {elemento.text}")
                        return True
                except:
                    continue
            
            # Verificar URL
            if 'login' not in self.driver.current_url.lower():
                logger.info("✅ Login exitoso (URL no es login)")
                return True
            
            logger.error("❌ Login no exitoso")
            return False
            
        except Exception as e:
            logger.error(f"Error verificando login: {e}")
            return False
        
    def verificar_ubicacion_rapida(self):
        """Verificación rápida de ubicación"""
        try:
            current_url = self.driver.current_url
            if 'corrientes' in current_url.lower():
                logger.info("📍 URL confirma ubicación Corrientes")
                return True
        except:
            pass
        return False

    def verificar_disponibilidad_corrientes(self):
        """Verifica si el producto está disponible en Corrientes"""
        try:
            # Buscar mensajes de no disponibilidad
            no_disponible = self.driver.find_elements(By.XPATH,
                "//*[contains(text(), 'no disponible') or contains(text(), 'sin stock')]")
            if no_disponible:
                for elem in no_disponible:
                    if elem.is_displayed():
                        return False
            return True
        except:
            return True