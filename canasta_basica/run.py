from carrefour_extractor import ExtractorCarrefour
from delimart_extractor import ExtractorDelimart
from load import load_canasta_basica_data
from utils_db import ConexionBaseDatos
from utils_sheets import ConexionGoogleSheets
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("canasta_basica")
class GestorCanastaBasica:
    """Gestiona todo el proceso de canasta básica"""
    
    def __init__(self):
        load_dotenv()
        self.supermercados = {
            'carrefour': ExtractorCarrefour(),
            'delimart' : ExtractorDelimart()
        }
        
        # Configuración de base de datos
        host = os.getenv('HOST_DBB')
        user = os.getenv('USER_DBB')
        password = os.getenv('PASSWORD_DBB')
        database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
        
        self.db = ConexionBaseDatos(host, user, password, database)
        self.db.connect_db()

    def leer_links_desde_sheets(self):
        """Lee todos los links desde Google Sheets"""
        load_dotenv()
        
        SHEET_ID = '13vz5WzXnXLdp61YVHkKO17C4OBEXVJcC5cP38N--8XA'
        # Rango configurable desde variables de entorno
        RANGO = os.getenv('SHEETS_RANGE', 'Hoja 1!A2:K60')
        #RANGO = os.getenv('SHEETS_RANGE', 'Hoja 1!A2:K13')

        gs = ConexionGoogleSheets(SHEET_ID)
        df_links = gs.leer_df(RANGO, header=False)
        
        productos = {}
        
        for idx, row in df_links.iterrows():
            if len(row) > 0 and pd.notna(row[0]):
                producto_nombre = row[0].strip()
                links = []
                
                for i in range(1, min(11, len(row))):
                    if pd.notna(row[i]) and isinstance(row[i], str) and row[i].startswith('http'):
                        links.append(row[i].strip())
                
                if links:
                    productos[producto_nombre] = links
                    logger.info(f"Producto: {producto_nombre} - {len(links)} links")
        
        return productos
    
    def inicializar_sesiones(self):
        """Inicializa las sesiones de todos los supermercados ANTES de empezar la extracción"""
        logger.info("Inicializando sesiones de supermercados...")
        
        for supermercado, extractor in self.supermercados.items():
            try:
                logger.info(f"Inicializando sesión para {supermercado}")
                
                # Usar el nuevo método asegurar_sesion_activa
                if hasattr(extractor, 'asegurar_sesion_activa'):
                    if extractor.asegurar_sesion_activa():
                        logger.info(f"Sesión activa para {supermercado}")
                    else:
                        logger.error(f"No se pudo establecer sesión para {supermercado}")
                else:
                    # Para extractores que no tienen el nuevo método
                    logger.warning(f"Extractor {supermercado} no tiene método asegurar_sesion_activa")
                    
            except Exception as e:
                logger.error(f"Error inicializando sesión para {supermercado}: {str(e)}")
    
    def procesar_supermercado(self, supermercado, productos_links):
        """Procesa todos los productos de un supermercado"""
        if supermercado not in self.supermercados:
            logger.error(f"Extractor no disponible para {supermercado}")
            return pd.DataFrame(), []
        
        extractor = self.supermercados[supermercado]
        todos_datos = []
        links_problematicos = []
        
        try:
            # VERIFICAR SI LA SESIÓN SIGUE ACTIVA (solo una vez por supermercado)
            if hasattr(extractor, 'sesion_iniciada') and not extractor.sesion_iniciada:
                logger.warning(f"Sesión perdida para {supermercado}, reintentando login...")
                if hasattr(extractor, 'asegurar_sesion_activa'):
                    if not extractor.asegurar_sesion_activa():
                        logger.error(f"No se pudo recuperar sesión para {supermercado}")
                        return pd.DataFrame(), []
            
            for producto_nombre, links in productos_links.items():
                logger.info(f"Procesando {producto_nombre} en {supermercado}")
                
                productos_procesados = []
                links_procesados = 0
                links_con_error = 0
                
                for url in links:
                    try:
                        # EXTRAER PRODUCTO (sin intentar login por cada uno)
                        datos = extractor.extraer_producto(url)
                        
                        if datos and 'error_type' in datos:
                            # Es un link problemático
                            datos['producto_principal'] = producto_nombre
                            links_problematicos.append(datos)
                            links_con_error += 1
                            logger.warning(f"⚠️ Link problemático ({datos['error_type']}): {url}")
                        elif datos:
                            datos['producto_principal'] = producto_nombre
                            productos_procesados.append(datos)
                            links_procesados += 1
                            logger.info(f"✅ {producto_nombre} extraído correctamente")
                        else:
                            links_con_error += 1
                            links_problematicos.append({
                                'producto_principal': producto_nombre,
                                'error_type': 'unknown',
                                'url': url,
                                'titulo': 'Error desconocido'
                            })
                            logger.warning(f"❌ No se pudieron extraer datos de {url}")
                        
                        # Pausa MÁS CORTA entre productos
                        time.sleep(0.5)  # Reducido de 1 a 0.5 segundos
                        
                    except Exception as e:
                        links_con_error += 1
                        links_problematicos.append({
                            'producto_principal': producto_nombre,
                            'error_type': 'exception',
                            'url': url,
                            'titulo': str(e),
                            'fecha': datetime.today().strftime("%Y-%m-%d")
                        })
                        logger.error(f"❌ Error procesando {url}: {str(e)}")
                        continue
                
                logger.info(f"📊 Resumen {producto_nombre}: {links_procesados} exitosos, {links_con_error} con error")
                
                if productos_procesados:
                    df_producto = pd.DataFrame(productos_procesados)
                    todos_datos.append(df_producto)
                    logger.info(f"✅ {producto_nombre} procesado: {len(df_producto)} registros")
                else:
                    logger.warning(f"⚠️ No se extrajeron datos para {producto_nombre}")
            
            if todos_datos:
                df_final = pd.concat(todos_datos, ignore_index=True)
                columnas = ['producto_principal'] + [col for col in df_final.columns if col != 'producto_principal']
                return df_final[columnas], links_problematicos
            return pd.DataFrame(), links_problematicos
            
        except Exception as e:
            logger.error(f"❌ Error crítico procesando {supermercado}: {str(e)}")
            # Marcar sesión como expirada para reintentar en la próxima ejecución
            if hasattr(extractor, 'sesion_iniciada'):
                extractor.sesion_iniciada = False
            return pd.DataFrame(), links_problematicos

    def generar_reporte_links_problematicos(self, links_problematicos):
        """
        Genera un reporte CSV con todos los links problemáticos encontrados.
        """
        try:
            if not links_problematicos:
                logger.info("✅ No hay links problemáticos para reportar")
                return
            
            # Crear DataFrame con links problemáticos
            df_problematicos = pd.DataFrame(links_problematicos)
            
            # Agregar timestamp
            df_problematicos['fecha_deteccion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Ordenar columnas si existen
            columnas_disponibles = df_problematicos.columns.tolist()
            columnas_orden = []
            
            # Agregar columnas en orden preferido si existen
            for col in ['supermercado', 'producto_principal', 'error_type', 'url', 'titulo', 'fecha_deteccion']:
                if col in columnas_disponibles:
                    columnas_orden.append(col)
            
            # Agregar cualquier columna restante
            for col in columnas_disponibles:
                if col not in columnas_orden:
                    columnas_orden.append(col)
            
            df_problematicos = df_problematicos[columnas_orden]
            
            # Guardar reporte
            archivo_reporte = f'links_problematicos_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            df_problematicos.to_csv(archivo_reporte, index=False, encoding='utf-8')
            
            # Estadísticas
            total_problemas = len(df_problematicos)
            
            logger.info(f"📊 REPORTE DE LINKS PROBLEMÁTICOS:")
            logger.info(f"   📄 Archivo: {archivo_reporte}")
            logger.info(f"   📊 Total problemas: {total_problemas}")
            
            if 'supermercado' in df_problematicos.columns:
                problemas_por_supermercado = df_problematicos.groupby('supermercado').size().to_dict()
                logger.info(f"   🏪 Por supermercado: {problemas_por_supermercado}")
            
            if 'error_type' in df_problematicos.columns:
                problemas_por_razon = df_problematicos.groupby('error_type').size().to_dict()
                logger.info(f"   ❌ Por tipo de error: {problemas_por_razon}")
            
        except Exception as e:
            logger.error(f"❌ Error generando reporte de links problemáticos: {e}")

    def run_canasta_basica(self):
        """Método principal que ejecuta todo el proceso - VERSIÓN MEJORADA"""
        logger.info("=" * 80)
        logger.info("Iniciando el proceso de ETL para CANASTA BASICA (SESIÓN PERSISTENTE)")
        logger.info("=" * 80)

        try:
            # 1. LEER LINKS DESDE GOOGLE SHEETS
            logger.info("Leyendo links desde Google Sheets...")
            productos_links = self.leer_links_desde_sheets()
            if not productos_links:
                logger.error("No se encontraron productos en Google Sheets")
                return
                        
            logger.info(f"Se encontraron {len(productos_links)} productos para procesar")
            
            # 2. INICIALIZAR SESIONES UNA SOLA VEZ AL INICIO
            self.inicializar_sesiones()

            # 1. EXTRAER TODAS LAS URLs EN UNA LISTA PLANA
            todas_las_urls = []
            for producto, urls in productos_links.items():
                todas_las_urls.extend(urls)
                logger.info(f"Producto: {producto} - {len(urls)} links")
            
            logger.info(f"Total de URLs a validar: {len(todas_las_urls)}")

            # 2. VALIDAR LINKS PRIMERO
            logger.info("=== FASE 1: VALIDACIÓN DE LINKS ===")
            extractor = ExtractorCarrefour()
            resultados_validacion = extractor.validar_links_productos(todas_las_urls)
            
            # 3. Mostrar resultados y preguntar si continuar
            links_validos = sum(1 for r in resultados_validacion.values() if r.get('valido', False))
            links_totales = len(todas_las_urls)
            
            print(f"\n📊 RESULTADO VALIDACIÓN: {links_validos}/{links_totales} links válidos")

            exit()
            
            # 3. PROCESAR CADA SUPERMERCADO
            todos_links_problematicos = []
            
            for supermercado in self.supermercados.keys():
                logger.info(f"{'='*50}")
                logger.info(f"🏪 PROCESANDO {supermercado.upper()}")
                logger.info(f"{'='*50}")
                
                inicio_supermercado = time.time()
                
                try:
                    df, links_problematicos = self.procesar_supermercado(supermercado, productos_links)
                    tiempo_supermercado = time.time() - inicio_supermercado
                    
                    # Agregar supermercado a links problemáticos
                    for link in links_problematicos:
                        link['supermercado'] = supermercado
                    todos_links_problematicos.extend(links_problematicos)
                    
                    if not df.empty:
                        logger.info(f"🎉 Extracción {supermercado} completada: {len(df)} registros en {tiempo_supermercado:.1f}s")
                        
                        # GUARDAR CSV DE PRODUCTOS EXITOSOS
                        csv_file = f'productos_{supermercado}_{datetime.today().strftime("%Y%m%d_%H%M")}.csv'
                        df.to_csv(csv_file, index=False, encoding='utf-8')
                        logger.info(f"📄 Datos guardados en {csv_file}")
                        
                        # CARGAR A BASE DE DATOS
                        try:
                            datos_nuevos = load_canasta_basica_data(df)
                            if datos_nuevos:
                                logger.info(f"💾 Carga a base completada: {len(df)} registros")
                            else:
                                logger.warning("⚠️ No se cargaron datos nuevos")
                        except Exception as e:
                            logger.error(f"❌ Error cargando a base de datos: {e}")
                        
                    else:
                        logger.warning(f"⚠️ No se extrajeron datos para {supermercado}")
                    
                    # MOSTRAR ESTADÍSTICAS
                    if links_problematicos:
                        logger.warning(f"⚠️ {len(links_problematicos)} links con problemas en {supermercado}")
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando {supermercado}: {str(e)}")
                    continue
            
            # 4. GENERAR REPORTE DE LINKS PROBLEMÁTICOS
            if todos_links_problematicos:
                self.generar_reporte_links_problematicos(todos_links_problematicos)
            
            # 4. GUARDAR SESIONES AL FINALIZAR
            self.guardar_sesiones()
                    
            logger.info("Proceso CANASTA BASICA finalizado exitosamente")
                
        except Exception as e:
            logger.error(f"Error en proceso principal: {str(e)}")
            raise
            
        finally:
            # 5. CERRAR TODOS LOS DRIVERS AL FINAL DEL PROCESO
            self._cerrar_drivers()

    def guardar_sesiones(self):
        """Guarda las sesiones de todos los supermercados"""
        logger.info("Guardando sesiones para futuras ejecuciones...")
        
        for supermercado, extractor in self.supermercados.items():
            try:
                if hasattr(extractor, 'guardar_sesion'):
                    if extractor.guardar_sesion():
                        logger.info(f"Sesión de {supermercado} guardada")
                    else:
                        logger.warning(f"No se pudo guardar sesión de {supermercado}")
            except Exception as e:
                logger.error(f"Error guardando sesión de {supermercado}: {str(e)}")

    def _cerrar_drivers(self):
        """Cierra todos los drivers de los extractores - VERSIÓN MEJORADA"""
        logger.info("Cerrando todos los drivers y guardando sesiones...")
        
        # Primero guardar sesiones
        self.guardar_sesiones()
        
        # Luego cerrar drivers
        for nombre, extractor in self.supermercados.items():
            try:
                # Usar el nuevo método 'cerrar' si existe
                if hasattr(extractor, 'cerrar'):
                    extractor.cerrar()
                    logger.info(f"Driver de {nombre} cerrado correctamente")
                elif hasattr(extractor, 'cerrar_driver'):
                    extractor.cerrar_driver()
                    logger.info(f"Driver de {nombre} cerrado (método antiguo)")
                elif hasattr(extractor, 'driver') and extractor.driver:
                    extractor.driver.quit()
                    logger.info(f"Driver de {nombre} cerrado (método directo)")
                else:
                    logger.info(f"ℹNo hay driver activo para {nombre}")
            except Exception as e:
                logger.error(f"Error cerrando driver de {nombre}: {str(e)}")

    def run_delimart(self):
        """Método principal que ejecuta todo el proceso - VERSIÓN MEJORADA"""
        logger.info("=" * 80)
        logger.info("Iniciando el proceso de ETL para CANASTA BASICA (SESIÓN PERSISTENTE)")
        logger.info("=" * 80)
        supermercado = 'delimart'
        try:
            extractor = ExtractorDelimart()
            links = ["https://www.delimart.com.ar/leche_entera_sachet_larga_vida_cremigal_1_lt"]
            
            # Extraer productos con manejo de errores
            df = extractor.extraer_lista_productos(links)
                    
            # Verificar correctamente el resultado
            if df is not None and not df.empty:
                logger.info(f"🎉 Extracción {supermercado} completada: {len(df)} productos")
                        
                # GUARDAR CSV DE PRODUCTOS EXITOSOS
                csv_file = f'productos_{supermercado}_{datetime.today().strftime("%Y%m%d_%H%M")}.csv'
                df.to_csv(csv_file, index=False, encoding='utf-8')
                logger.info(f"📄 Datos guardados en {csv_file}")
            else:
                logger.warning(f"⚠ No se pudieron extraer datos de {supermercado}")
                # Crear DataFrame vacío para evitar errores
                df = pd.DataFrame()
                        
        except Exception as e:
            logger.error(f"❌ Error procesando {supermercado}: {str(e)}")
            # Asegurar que df existe incluso en caso de error
            df = pd.DataFrame()

def run_canasta_basica():
    """Función wrapper para mantener la compatibilidad con imports existentes"""
    gestor = GestorCanastaBasica()
    
    try:
        gestor.run_delimart()
    except Exception as e:
        logger.error(f"Error crítico en run_canasta_basica: {str(e)}")
        raise
    finally:
        # Asegurar que los drivers se cierren incluso si hay error
        gestor._cerrar_drivers()

if __name__ == "__main__":
    """Ejecutar cuando el script se llama directamente"""
    print("🚀 Iniciando proceso CANASTA BASICA optimizado...")
    run_canasta_basica()