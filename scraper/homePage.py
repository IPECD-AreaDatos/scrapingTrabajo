import requests
from bs4 import BeautifulSoup
import urllib3

class HomePage:

    url = "https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad"

    def getDownloadUrl(self):
        
        urllib3.disable_warnings()
        response = requests.get(self.url, verify=False)
        response.raise_for_status()

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            button = soup.find("button", text="DESCARGAR")
            print("button ->", button)
            parent_a = button.find_parent("a")
            print("parent ->", parent_a)
            href = parent_a.get("href")
            print("href:", href)
            return href
        else:
            print("Fallo...")
            return ""

