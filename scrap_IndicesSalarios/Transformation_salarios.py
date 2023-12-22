#Objetivo: Obtener los datos del excel descargado, y brindarle el formato adecuado
import os
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta


class Transformation_Data:

    def contruccion_df(self):

        path_archivo = self.direccion_archivo()
        print(path_archivo)

    def direccion_archivo(self):

        # Direccion actual + nombre archivo = direccion del archivo
        directorio_actual  = os.path.dirname(os.path.abspath(__file__))
        nombre_archivo = '\\files\\indices_salarios.xls'
        path_archivo = directorio_actual + nombre_archivo
        return path_archivo


Transformation_Data().contruccion_df()