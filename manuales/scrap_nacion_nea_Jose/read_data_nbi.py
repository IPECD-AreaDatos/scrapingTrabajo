import pandas as pd

class readData:
    def read_data_nbi(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[0]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
    
    def read_data_inmat(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[1]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
    
    def read_data_agua(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[2]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
    
    def read_data_combustible(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[3]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)

    def read_data_internet(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[4]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
            
    def read_data_clima_educativo(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[5]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
            
    def read_data_educativo_mayor_25(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[6]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)

    def read_data_cobertura_salud(self, direccion_archivo):
        try:
            # Carga todas las hojas si hay más de una
            excel_data = pd.ExcelFile(direccion_archivo)
            
            # Selecciona la hoja que deseas cargar (por ejemplo, la primera)
            sheet_name = excel_data.sheet_names[7]  # Cambia el índice o el nombre según la hoja deseada
            df = pd.read_excel(direccion_archivo, sheet_name=sheet_name)
            return df
        except Exception as e:
            print("Error al leer el archivo Excel:", e)