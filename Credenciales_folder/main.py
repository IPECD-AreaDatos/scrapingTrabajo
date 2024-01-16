import os

if __name__ == '__main__':
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    nuevo_directorio = os.path.join(directorio_desagregado, "..")
    nuevo_directorio = os.path.abspath(nuevo_directorio)
    ruta_carpeta_files = os.path.join(nuevo_directorio, 'Scrap_IPC')
    ruta_carpeta_files = os.path.join(ruta_carpeta_files, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    print("aca ", file_path_desagregado)
    #ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    #file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')