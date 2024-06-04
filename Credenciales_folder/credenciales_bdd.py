"""
En este archivos vamos a almacenar las credenciales de la BDD.
Esto para que cada cambio de IP o cambio de  credenciales, no impacte
tanto en los arreglos posteriores.

"""

#Clase orientada a contener las credenciales de la BDD
class Credenciales:
    def __init__(self, database_name):
        # Datos de las bases de datos
        if database_name == 'ipecd_economico':
            self.host = '54.94.131.196'
            self.user = 'estadistica'
            self.password = 'Estadistica2024!!'
            self.database = 'ipecd_economico'
        elif database_name == 'datalake_sociodemografico':
            self.host = '54.94.131.196'
            self.user = 'estadistica'
            self.password = 'Estadistica2024!!'
            self.database = 'datalake_sociodemografico'
        elif database_name == 'dwh_sociodemografico':
            self.host = '54.94.131.196'
            self.user = 'estadistica'
            self.password = 'Estadistica2024!!'
            self.database = 'dwh_sociodemografico'
        elif database_name == 'datalake_economico':
            self.host = '54.94.131.196'
            self.user = 'estadistica'
            self.password = 'Estadistica2024!!'
            self.database = 'datalake_economico'
        elif database_name == 'dwh_economico':
            self.host = '54.94.131.196'
            self.user = 'estadistica'
            self.password = 'Estadistica2024!!'
            self.database = 'dwh_economico'
        elif database_name == 'local_ipecd_economico':
            self.host = '172.17.22.23'
            self.user = 'team-datos'
            self.password = 'HCj_BmbCtTuCv5}'
            self.database = 'ipecd_economico'
        elif database_name == 'local_dwh_sociodemografico':
            self.host = '172.17.22.23'
            self.user = 'team-datos'
            self.password = 'HCj_BmbCtTuCv5}'
            self.database = 'dwh_sociodemografico'
        elif database_name == 'local_dwh_economico':
            self.host = '172.17.22.23'
            self.user = 'team-datos'
            self.password = 'HCj_BmbCtTuCv5}'
            self.database = 'dwh_economico'
        elif database_name == 'local_datalake_sociodemografico':
            self.host = '172.17.22.23'
            self.user = 'team-datos'
            self.password = 'HCj_BmbCtTuCv5}'
            self.database = 'datalake_sociodemografico'
        elif database_name == 'local_datalake_economico':
            self.host = '172.17.22.23'
            self.user = 'team-datos'
            self.password = 'HCj_BmbCtTuCv5}'
            self.database = 'datalake_economico'
        elif database_name == 'reconocimientos_medicos':
            self.host = 'salocup.ddns.net'
            self.user = 'ex30216194'
            self.password = '5f1i6nFpMv5O'
            self.database = ''
        else:
            raise ValueError("Nombre de base de datos no válido")

        #Asi se debe colocar
        """
        import os
        import sys 
        # Obtener la ruta al directorio actual del script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
        # Agregar la ruta al sys.path
        sys.path.append(credenciales_dir)
        
        credenciales = Credenciales()
    
        credenciales.host, credenciales.user, credenciales.password, credenciales.database
        """