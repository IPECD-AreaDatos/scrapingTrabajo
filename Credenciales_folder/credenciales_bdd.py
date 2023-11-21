"""
En este archivos vamos a almacenar las credenciales de la BDD.
Esto para que cada cambio de IP o cambio de  credenciales, no impacte
tanto en los arreglos posteriores.

"""

#Clase orientada a contener las credenciales de la BDD
class Credenciales:
        
    def __init__(self):

        #Datos de la base de datos
        self.host = '172.17.22.23'
        self.user = 'team-datos'
        self.password = 'HCj_BmbCtTuCv5}'
        self.database = 'ipecd_economico'

