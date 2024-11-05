import os
import sys
import pandas as pd

class readSheet:
    def read_data_agua(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Agua beber o cocinar.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_vivienda', 'red_publica', 'bomba_motor', 'bomba_manual', 'pozo_sin_bomba', 'cisterna_canal_acequia', 'otra']
        df = df.reset_index(drop=True)
        
        print(df)
        return df

    def read_data_cloaca(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Cloaca.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'red_publica', 'camara_septica_con_pozo_ciego', 'solo_pozo_ciego', 'hoyo_excavacion_etc']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_combustible(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Combustible para cocinar.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'electricidad', 'gas_red', 'gas_tubo_o_zepelin', 'gas_garrafa', 'leña_carbon', 'otro']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_inmat(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base INMAT.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'calidad_1', 'calidad_2', 'calidad_3', 'calidad_4', 'ignorado']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_internet(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Internet.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'con_internet', 'sin_internet']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_material_de_piso(self):  
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Material del Piso.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'ceramica_mosaico_baldosa_etc', 'carpeta_contrapiso_ladrillo', 'tierra_ladrillosuelto', 'otro']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_nbi(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base NBI.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'si_nbi_total', 'no_nbi_total', 'si_nbi_hacinamiento', 'no_nbi_hacinamiento', 'si_nbi_viv_incoveniente', 'no_nbi_viv_incoveniente', 'si_nbi_cond_sanitarias', 'no_nbi_cond_sanitarias', 'si_nbi_escolaridad', 'no_nbi_escolaridad', 'si_nbi_capacidad_subsistencia', 'no_nbi_capacidad_subsistencia']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_p18_corrientes(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base P18 Corrientes.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['depto_p18', 'municipio_p1', 'codigo_p18']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_poblacion_viviendas(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Población-Viviendas.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'poblacion']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_propiedad_de_la_vivienda(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Propiedad de la vivienda.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'propia', 'alquilada', 'cedida_por_trabajo', 'prestada', 'otra_situacion']
        df = df.reset_index(drop=True)
        print(df)
        return df
    
    def read_data_tenencia_de_agua(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Base Tenencia de Agua.xlsx')
        
        df = pd.read_excel(file_path_desagregado, skiprows=0)
        df = df.dropna()
        df.columns = ['codigo', 'total_viviendas', 'cañeria_dentro_viv', 'fuera_viv_dentro_terreno', 'fuera_terreno']
        df = df.reset_index(drop=True)
        print(df)
        return df