import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from datetime import datetime
import calendar
import os
import xlrd
from sqlalchemy import create_engine

class conexionBaseDatos:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def cargaBaseDatos_agua(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_agua_beber_o_cocinar", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_cloaca(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_cloaca", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_combustible(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_combustible_para_cocinar", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_inmat(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_inmat", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_internet(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_internet", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_material_piso(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_material_piso", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_nbi(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_nbi", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_p18_corrientes(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_p18_corrientes", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_poblacion_viviendas(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_poblacion_viviendas", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cargaBaseDatos_propiedad_vivienda(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_propiedad_de_la_vivienda", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_tenencia_agua(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_tenencia_de_agua", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    #=== se AGREGO EL 19/11

    def cargaBaseDatos_asistencia_escolar(self, df):
    # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_asistencia_escolar", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_categoria_ocupacional(self, df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_categoria_ocupacional", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_cobertura_salud(self, df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_cobertura_salud", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_nivel_educativo_mayores_25(self, df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_nivel_educativo_mayores_25", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_tasas_mercado_trabajo(self, df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_tasas_mercado_trabajo", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
    
    #SE AGREGO 22/11
    
    def cargarBaseDatos_clima_educativo_municipio_2022(self,df):

        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_clima_educativo_hogar", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    # Se agrego 26/11

    def cargaBaseDatos_piramide_2022(self,df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_piramide_2022", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_piramide_2010(self,df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_piramide_2010", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_piramide_2001(self,df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_piramide_2001", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_piramide_1991(self,df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_piramide_1991", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()

    def cargaBaseDatos_piramide_1980(self,df):
        # Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="base_piramide_1980", con=engine, if_exists='replace', index=False)
        print("Base actualizada")
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        
    def cerrar_conexion(self):
        # Cierra el cursor y la conexión si están abiertos
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Conexión cerrada")