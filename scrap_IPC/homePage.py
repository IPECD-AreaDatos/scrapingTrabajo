import requests
from bs4 import BeautifulSoup
import urllib3

class HomePage:
    #Contiene la URL de la página web desde donde se desea obtener la URL de descarga del archivo.
    url = "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-5-31"
    
    #Se encarga de obtener la URL de descarga del archivo.
    def getDownloadUrl(self):
        
        urllib3.disable_warnings()#Se desactivan las advertencias de verificación de certificados SSL.
        response = requests.get(self.url, verify=False)#Se realiza una solicitud GET a la URL especificada y se guarda la respuesta 
        response.raise_for_status()#Verifica si la respuesta de la solicitud tiene un código de estado exitoso

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            print("", response-conte)
            tag_a = soup.find("a")
            #Se obtiene el valor del atributo href del elemento "a", que representa la URL de descarga del archivo.
            href = tag_a.get("href")
            print("href:", href)
            return href
        else:
            print("Fallo...")
            return ""

