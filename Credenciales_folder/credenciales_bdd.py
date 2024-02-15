"""
En este archivos vamos a almacenar las credenciales de la BDD.
Esto para que cada cambio de IP o cambio de  credenciales, no impacte
tanto en los arreglos posteriores.

"""

#Clase orientada a contener las credenciales de la BDD
class Credenciales:
        
    def __init__(self):

        #Datos de la base de datos
        self.host = '54.94.131.196'
        self.user = 'estadistica'
        self.password = 'Estadistica2024!!'
        self.database = 'ipecd_economico'

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