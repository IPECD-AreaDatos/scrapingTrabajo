import requests
import os
import urllib3
import zipfile
import shutil

class HomePage:
    
    def __init__(self):
        """Inicializa la clase para descargar archivos de ANAC"""
        self.url_base = 'https://datos.anac.gob.ar/estadisticas/'
        self.url_descarga = 'https://docs.anac.gob.ar/index.php/s/4ptegdXanm2rWJG/download'

    def descargar_archivo(self):
        """
        Descarga el archivo comprimido de estadisticas de la ANAC y lo descomprime

        Descarga el archivo comprimido de estadisticas de la ANAC, lo descomprime y extrae el archivo Excel.

        :return: None
        """
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # URL directa del archivo comprimido
            url_archivo_zip = self.url_descarga
            
            # Obtener la ruta del directorio actual (donde se encuentra el script)
            directorio_actual = os.path.dirname(os.path.abspath(__file__))
            # Construir la ruta de la carpeta "files" dentro del directorio actual
            carpeta_guardado = os.path.join(directorio_actual, 'files')

            # Crear la carpeta "files" si no existe
            if not os.path.exists(carpeta_guardado):
                os.makedirs(carpeta_guardado)
                print(f"Carpeta creada: {carpeta_guardado}")

            # Nombres de archivos
            nombre_archivo_zip = 'anac_estadisticas.zip'
            ruta_zip = os.path.join(carpeta_guardado, nombre_archivo_zip)
            nombre_archivo_final = 'anac.xlsx'
            ruta_excel_final = os.path.join(carpeta_guardado, nombre_archivo_final)

            # Verificar espacio libre antes de descargar (crítico para EC2)
            self._verificar_espacio_disco(carpeta_guardado)
            
            # Verificar si el archivo Excel ya existe y es reciente (menos de 1 hora)
            if os.path.exists(ruta_excel_final):
                import time
                tiempo_archivo = os.path.getmtime(ruta_excel_final)
                tiempo_actual = time.time()
                if (tiempo_actual - tiempo_archivo) < 3600:  # 1 hora = 3600 segundos
                    print(f"✓ Usando archivo Excel existente (descargado hace menos de 1 hora)")
                    return

            # Descargar el archivo ZIP con reintentos
            print(f"Descargando archivo comprimido desde: {url_archivo_zip}")
            max_intentos = 3
            for intento in range(max_intentos):
                try:
                    response = requests.get(url_archivo_zip, verify=False, timeout=60)
                    break
                except requests.exceptions.RequestException as e:
                    if intento < max_intentos - 1:
                        print(f"Intento {intento + 1} falló: {e}. Reintentando...")
                        continue
                    else:
                        raise Exception(f"Error después de {max_intentos} intentos: {e}")

            # Verificar que la descarga fue exitosa
            if response.status_code == 200:
                # Guardar el archivo ZIP
                with open(ruta_zip, 'wb') as file:
                    file.write(response.content)
                print(f"✓ Archivo comprimido descargado: {ruta_zip}")
                print(f"Tamaño del archivo: {len(response.content)} bytes")

                # Descomprimir el archivo ZIP
                print("Descomprimiendo archivo...")
                try:
                    with zipfile.ZipFile(ruta_zip, 'r') as zf:
                        # Listar contenidos del ZIP
                        contenidos = zf.namelist()
                        print(f"Archivos en el ZIP: {contenidos}")
                        
                        # Buscar archivo Excel más reciente (2023-2025)
                        archivo_excel = None
                        archivos_excel = []
                        
                        # Encontrar todos los archivos Excel
                        for archivo in contenidos:
                            if archivo.lower().endswith(('.xlsx', '.xls')):
                                archivos_excel.append(archivo)
                        
                        # Priorizar el archivo más reciente (2023-2025)
                        for archivo in archivos_excel:
                            if '2023-2025' in archivo:
                                archivo_excel = archivo
                                break
                        
                        # Si no se encuentra 2023-2025, usar el primero disponible
                        if archivo_excel is None and archivos_excel:
                            archivo_excel = archivos_excel[0]
                            
                        print(f"Archivos Excel disponibles: {archivos_excel}")
                        
                        if archivo_excel:
                            print(f"Archivo Excel encontrado: {archivo_excel}")
                            # Extraer el archivo Excel
                            zf.extract(archivo_excel, carpeta_guardado)
                            
                            # Renombrar el archivo extraído si es necesario
                            ruta_excel_extraido = os.path.join(carpeta_guardado, archivo_excel)
                            if ruta_excel_extraido != ruta_excel_final:
                                shutil.move(ruta_excel_extraido, ruta_excel_final)
                                print(f"Archivo renombrado a: {ruta_excel_final}")
                            
                            print(f"✓ Archivo Excel extraído exitosamente: {ruta_excel_final}")
                            
                            # Limpiar archivos temporales
                            self._limpiar_archivos_temporales(carpeta_guardado, ruta_zip)
                                
                        else:
                            raise Exception("No se encontró ningún archivo Excel en el ZIP")
                            
                except zipfile.BadZipFile as e:
                    print(f"Error: El archivo descargado no es un ZIP válido: {e}")
                    # Intentar verificar si es otro tipo de archivo
                    print("Verificando tipo de archivo...")
                    with open(ruta_zip, 'rb') as f:
                        header = f.read(10)
                        print(f"Header del archivo: {header}")
                    raise
                    
            else:
                print(f"Error al descargar el archivo. Código de respuesta: {response.status_code}")
                raise Exception(f"Error HTTP {response.status_code} al descargar el archivo")

        except Exception as e:
            print(f"Se produjo un error en la descarga: {e}")
            raise

        finally:
            # No necesitamos cerrar driver aquí ya que no lo usamos
            pass
    
    def _limpiar_archivos_temporales(self, carpeta_guardado, ruta_zip):
        """Limpia archivos temporales y directorios extraídos (optimizado para EC2)"""
        archivos_eliminados = 0
        espacio_liberado = 0
        
        try:
            # Eliminar ZIP temporal
            if os.path.exists(ruta_zip):
                size = os.path.getsize(ruta_zip)
                os.remove(ruta_zip)
                espacio_liberado += size
                archivos_eliminados += 1
                print(f"✓ ZIP temporal eliminado ({size/1024/1024:.1f}MB)")
            
            # Eliminar directorio de extracción temporal si existe
            directorio_temporal = os.path.join(carpeta_guardado, "tablas de movimientos y pasajeros")
            if os.path.exists(directorio_temporal):
                # Calcular tamaño antes de eliminar
                for root, dirs, files in os.walk(directorio_temporal):
                    for file in files:
                        filepath = os.path.join(root, file)
                        if os.path.exists(filepath):
                            espacio_liberado += os.path.getsize(filepath)
                            archivos_eliminados += 1
                
                shutil.rmtree(directorio_temporal)
                print(f"✓ Directorio temporal eliminado")
            
            # Verificar espacio libre en disco (crítico para EC2)
            self._verificar_espacio_disco(carpeta_guardado)
            
            if archivos_eliminados > 0:
                print(f"✓ Limpieza completada: {archivos_eliminados} archivos, {espacio_liberado/1024/1024:.1f}MB liberados")
                
        except PermissionError:
            print("⚠ No se pudieron eliminar algunos archivos temporales (en uso)")
        except Exception as e:
            print(f"⚠ Advertencia limpiando temporales: {e}")
    
    def _verificar_espacio_disco(self, carpeta_guardado):
        """Verifica espacio libre en disco (crítico para EC2)"""
        try:
            stat = shutil.disk_usage(carpeta_guardado)
            espacio_libre_gb = stat.free / (1024**3)
            espacio_total_gb = stat.total / (1024**3)
            porcentaje_libre = (stat.free / stat.total) * 100
            
            print(f"💾 Espacio libre: {espacio_libre_gb:.1f}GB de {espacio_total_gb:.1f}GB ({porcentaje_libre:.1f}%)")
            
            # Alerta si queda menos de 1GB o menos del 10%
            if espacio_libre_gb < 1.0 or porcentaje_libre < 10:
                print(f"⚠️  ADVERTENCIA: Poco espacio en disco ({espacio_libre_gb:.1f}GB libre)")
                
        except Exception as e:
            print(f"⚠ No se pudo verificar espacio en disco: {e}")
