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

            # Descargar el archivo ZIP
            print(f"Descargando archivo comprimido desde: {url_archivo_zip}")
            response = requests.get(url_archivo_zip, verify=False, timeout=60)

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
                            
                            # Limpiar archivo ZIP temporal
                            try:
                                if os.path.exists(ruta_zip):
                                    os.remove(ruta_zip)
                                    print("Archivo ZIP temporal eliminado")
                            except PermissionError:
                                print("No se pudo eliminar el archivo ZIP temporal (archivo en uso)")
                            except Exception as e:
                                print(f"Advertencia: No se pudo eliminar archivo temporal: {e}")
                                
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
