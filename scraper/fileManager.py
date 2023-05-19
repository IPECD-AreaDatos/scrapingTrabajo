import requests
import os
from datetime import datetime
import urllib3
from pathlib import Path

class FileManager:
    
    url = ""
    folder = str(Path.cwd()) + "/scraper/files/csv"

    def __init__(self, url):
        self.url = url

    def downloadCSV(self):

        urllib3.disable_warnings()
        response = requests.get(self.url, verify=False)
        response.raise_for_status()

        if response.status_code == 200:
            csv_content = response.content
            timestamp = str(datetime.now().timestamp()).replace(".", "_")
            file_name = timestamp + ".csv"
            file_path = os.path.join(self.folder, file_name)

            with open(file_path, "wb") as file:
                file.write(csv_content)

            print("Se guardo el archivo")
            return file_name
        else:
            print("Fallo la desacaga del archivo..")

