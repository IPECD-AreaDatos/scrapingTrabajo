from pymysql import connect
from sqlalchemy import create_engine

class conexcionBaseDatos:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga(self, df):

        #Obtencion del tamano de los datos de la bdd
        select_row_count_query = f"SELECT COUNT(*) FROM ipc_valores"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df) 

        return tamano_bdd,tamano_df
    
    #Objetivo: realizar efectivamente la carga a la BDD
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print(f"*************************Inicio de la seccion IPC VALORES *********************************")
        print("\n*****************************************************************************")

        # Obtén los registros únicos en la base de datos
        select_unique_query = f"SELECT fecha, id_region FROM ipc_valores"
        self.cursor.execute(select_unique_query)
        registros_bdd = self.cursor.fetchall()
        registros_bdd_set = set(registros_bdd)  # Convertir a un conjunto para búsqueda rápida

        # Filtrar nuevos registros en el DataFrame
        nuevos_registros = df[~df.set_index(['fecha', 'id_region']).index.isin(registros_bdd_set)]

        print(f" - Cantidad de datos en la base de datos: {len(registros_bdd)}")
        print(f" - Cantidad de datos extraidos: {len(df)}")
        print(f" - Nuevos registros a insertar: {len(nuevos_registros)}")

        # Si hay nuevos registros, cargarlos en la base de datos
        if not nuevos_registros.empty:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            nuevos_registros.to_sql(name='ipc_valores', con=engine, if_exists='append', index=False)

            print("*************")
            print(f" == ACTUALIZACION == Nuevos datos en la base de IPC VALORES")
            print("*************")
            return True
        else:
            print("*********")
            print(f"No existen datos nuevos de IPC VALORES")
            print("*********")
            return False


    def main(self, df):

        #Nos conectamos a la BDD
        self.conectar_bdd()

        #Realizamos carga, o no, y asignamos valor a la bandera
        bandera = self.cargaBaseDatos(df)

        #Cerramos conexiones
        self.conn.commit()
        self.conn.close()
        self.cursor.close()

        return bandera


