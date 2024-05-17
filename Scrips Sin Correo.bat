@echo off 

echo Comenzando la ejecucion
title Programa datos

echo ------------------------------------
echo DOLAR
echo ------------------------------------
python C:\Users\Usuario\Desktop\scrapingTrabajo\scrap_DOLAR\main.py
echo ------------------------------------
echo Termino DOLAR
echo ------------------------------------
echo ------------------------------------
echo Supermercados
echo ------------------------------------
python C:\Users\Usuario\Desktop\scrapingTrabajo\scrap_Supermercados\main.py
echo ------------------------------------
echo Termino CANASTA BASICA Y CANASTA TOTAL
echo ------------------------------------
echo ------------------------------------
echo IPICORR
echo ------------------------------------
python C:\Users\Usuario\Desktop\scrapingTrabajo\scrap_IPICORR\main.py
echo ------------------------------------
echo Termino IPICOR 
echo ------------------------------------
python C:\Users\Usuario\Desktop\scrapingTrabajo\scrap_semaforo\main.py
echo ------------------------------------
echo Termino INDICADORES SEMAFOROS
echo ------------------------------------
echo ------------------------------------
echo INDICADORES SALARIOS
echo ------------------------------------
python C:\Users\Usuario\Desktop\scrapingTrabajo\indice_salarios\main.py
echo ------------------------------------
echo Termino INDICADORES SALARIOS
echo ------------------------------------


echo Termino la ejecucion