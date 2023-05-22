import requests
from bs4 import BeautifulSoup
import urllib3

class HomePage:
    #Contiene la URL de la página web desde donde se desea obtener la URL de descarga del archivo.
    url = "https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad"
    
    #Se encarga de obtener la URL de descarga del archivo.
    def getDownloadUrl(self):
        
        urllib3.disable_warnings()#Se desactivan las advertencias de verificación de certificados SSL.
        response = requests.get(self.url, verify=False)#Se realiza una solicitud GET a la URL especificada y se guarda la respuesta 
        response.raise_for_status()#Verifica si la respuesta de la solicitud tiene un código de estado exitoso

        if response.status_code == 200:
            #Se crea un objeto BeautifulSoup a partir del contenido HTML/Analizar la estructura del documento
            soup = BeautifulSoup(response.content, "html.parser") 
            #Se busca el elemento button que contiene el texto "DESCARGAR".
            button = soup.find("button", text="DESCARGAR")
            print("button ->", button)
            #Se busca el elemento "a" que es el padre del elemento button.
            parent_a = button.find_parent("a")
            print("parent ->", parent_a)
            #Se obtiene el valor del atributo href del elemento "a", que representa la URL de descarga del archivo.
            href = parent_a.get("href")
            print("href:", href)
            return href
        else:
            print("Fallo...")
            return ""

