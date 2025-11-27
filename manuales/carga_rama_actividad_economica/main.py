import pandas as pd
import os
from dotenv import load_dotenv
from pymysql import connect
from sqlalchemy import create_engine, inspect
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CargaRamaActividadEconomica:
    def __init__(self):
        # Cargar variables de entorno
        load_dotenv()
        
        # Validar variables de entorno
        required_env_vars = ['HOST_DBB', 'USER_DBB', 'PASSWORD_DBB', 'NAME_DBB_DWH_SOCIO']
        for var in required_env_vars:
            if not os.getenv(var):
                logging.error(f"La variable de entorno {var} no está definida.")
                raise ValueError(f"Variable de entorno {var} faltante")
        
        self.host = os.getenv('HOST_DBB')
        self.user = os.getenv('USER_DBB')
        self.password = os.getenv('PASSWORD_DBB')
        self.database = os.getenv('NAME_DBB_DWH_SOCIO')
        
        # Ruta del archivo
        self.directorio_actual = os.path.dirname(os.path.abspath(__file__))
        self.ruta_archivo = os.path.join(self.directorio_actual, 'files', 'Rama de Actividad Económica por municipios 2022.xlsx')
    
    def leer_datos(self):
        """Leer y procesar los datos del archivo Excel"""
        try:
            logging.info("Leyendo datos del archivo Excel...")
            
            # Leer el archivo Excel
            df = pd.read_excel(self.ruta_archivo, sheet_name='Datos Indec')
            
            # Mapear nombres de columnas del Excel a nombres de la tabla BD
            column_mapping = {
                'codigo': 'codigo',
                'Educación o salud privada': 'educacion_o_salud_privada',
                'Empresa de electricidad, gas o agua': 'empresa_de_electricidad_gas_o_agua',
                'Otros servicios personales, técnicos o profesionales': 'otros_servicios_personales_tecnicos_o_profesionales',
                'Educación o salud sin determinar si es pública o privada': 'educacion_o_salud_sin_determinar_si_es_publica_o_privada',
                'Administración pública, educación o salud pública': 'administracion_publica_educacion_o_salud_publica',
                'Comercio': 'comercio',
                'Industria': 'industria',
                'Agropecuaria, pesca o minería': 'agropecuaria_pesca_o_mineria',
                'Construcción': 'construccion',
                'Transporte o almacenamiento': 'transporte_o_almacenamiento',
                'Hotel o restaurante': 'hotel_o_restaurante',
                'Banco, financiera o aseguradora': 'banco_financiera_o_aseguradora',
                'Ignorado': 'ignorado'
            }
            
            # Renombrar columnas
            df = df.rename(columns=column_mapping)
            
            logging.info(f"Datos leídos exitosamente: {len(df)} registros")
            logging.info(f"Columnas: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error al leer los datos: {e}")
            raise
    
    def conectar_bd(self):
        """Establecer conexión con la base de datos"""
        try:
            logging.info("Conectando a la base de datos...")
            
            # Crear conexión con pymysql
            self.conn = connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            
            # Crear engine para SQLAlchemy
            self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")
            
            logging.info("Conexión establecida exitosamente")
            
        except Exception as e:
            logging.error(f"Error al conectar con la base de datos: {e}")
            raise
    
    def verificar_tabla_existe(self):
        """Verificar si la tabla rama_actividad_economica existe"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            if 'rama_actividad_economica' in tables:
                logging.info("La tabla rama_actividad_economica ya existe")
                return True
            else:
                logging.warning("La tabla rama_actividad_economica NO existe")
                return False
                
        except Exception as e:
            logging.error(f"Error al verificar la tabla: {e}")
            raise
    
    def verificar_datos_existentes(self, df):
        """Verificar qué datos ya existen en la base de datos"""
        try:
            # Obtener códigos existentes
            query = "SELECT codigo FROM rama_actividad_economica"
            df_existentes = pd.read_sql(query, self.conn)
            
            if df_existentes.empty:
                logging.info("No hay datos existentes en la tabla")
                return df
            
            # Filtrar solo los códigos que no existen
            codigos_existentes = set(df_existentes['codigo'])
            df_nuevos = df[~df['codigo'].isin(codigos_existentes)]
            
            logging.info(f"Códigos existentes en BD: {len(codigos_existentes)}")
            logging.info(f"Registros nuevos a cargar: {len(df_nuevos)}")
            
            if len(df_nuevos) == 0:
                logging.info("No hay registros nuevos para cargar")
            
            return df_nuevos
            
        except Exception as e:
            logging.error(f"Error al verificar datos existentes: {e}")
            # Si hay error, cargar todos los datos (puede que la tabla esté vacía)
            return df
    
    def cargar_datos(self, df):
        """Cargar datos a la base de datos"""
        try:
            if df.empty:
                logging.info("No hay datos para cargar")
                return False
            
            logging.info(f"Cargando {len(df)} registros a la base de datos...")
            
            # Usar to_sql para cargar los datos
            df.to_sql(
                name='rama_actividad_economica',
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi'  # Para mejorar performance
            )
            
            # Commit la transacción
            self.conn.commit()
            
            logging.info("Datos cargados exitosamente")
            return True
            
        except Exception as e:
            logging.error(f"Error al cargar los datos: {e}")
            self.conn.rollback()
            raise
    
    def cerrar_conexion(self):
        """Cerrar conexiones a la base de datos"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
            if hasattr(self, 'engine') and self.engine:
                self.engine.dispose()
            
            logging.info("Conexiones cerradas")
            
        except Exception as e:
            logging.error(f"Error al cerrar conexiones: {e}")
    
    def ejecutar_carga(self):
        """Ejecutar el proceso completo de carga"""
        try:
            # 1. Leer datos del archivo Excel
            df = self.leer_datos()
            
            # 2. Conectar a la base de datos
            self.conectar_bd()
            
            # 3. Verificar si la tabla existe
            tabla_existe = self.verificar_tabla_existe()
            
            if not tabla_existe:
                logging.error("La tabla rama_actividad_economica no existe. Debe crearla primero.")
                return False
            
            # 4. Verificar datos existentes y filtrar nuevos
            df_nuevos = self.verificar_datos_existentes(df)
            
            # 5. Cargar datos nuevos
            carga_exitosa = self.cargar_datos(df_nuevos)
            
            # 6. Mostrar resumen
            if carga_exitosa:
                logging.info("=" * 50)
                logging.info("CARGA COMPLETADA EXITOSAMENTE")
                logging.info(f"Registros procesados: {len(df)}")
                logging.info(f"Registros cargados: {len(df_nuevos)}")
                logging.info("=" * 50)
            
            return carga_exitosa
            
        except Exception as e:
            logging.error(f"Error durante el proceso de carga: {e}")
            return False
            
        finally:
            self.cerrar_conexion()

def main():
    """Función principal"""
    try:
        logging.info("Iniciando carga de datos de Rama de Actividad Económica...")
        
        # Crear instancia y ejecutar carga
        cargador = CargaRamaActividadEconomica()
        exito = cargador.ejecutar_carga()
        
        if exito:
            logging.info("Proceso completado exitosamente")
        else:
            logging.error("El proceso falló")
            
    except Exception as e:
        logging.error(f"Error en el proceso principal: {e}")

if __name__ == "__main__":
    main()
