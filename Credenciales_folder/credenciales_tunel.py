#Clase orientada a contener las credenciales de tunelizacion y MySQL

class CredencialesTunel:
        
    def __init__(self):

        #Credenciales de AWS
        self.ssh_host = "54.94.131.196"
        self.ssh_user = "ubuntu"
        self.ssh_pem_key_path = "Credenciales_folder\\key_ssh_ipecd.pem"

        #Credeenciales de Base de datos
        self.mysql_host = "127.0.0.1"
        self.mysql_port = 3306
        self.mysql_user = "root" 
        self.mysql_password = "4#1P3C3D_!@@"