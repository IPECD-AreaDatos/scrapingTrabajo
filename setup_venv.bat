@echo off
echo -----------------------------------------
echo  🔧 Iniciando configuracion del entorno virtual...
echo -----------------------------------------

REM Crear entorno si no existe
IF NOT EXIST "venv\" (
    echo 📦 Creando entorno virtual en .\venv ...
    python -m venv venv
) ELSE (
    echo ✅ Entorno virtual ya existe
)

REM Activar entorno
echo ⚙️ Activando entorno virtual...
call venv\Scripts\activate.bat

REM Instalar requirements si existe
IF EXIST "requirements.txt" (
    echo 📥 Instalando paquetes desde requirements.txt ...
    pip install -r requirements.txt
) ELSE (
    echo ⚠️ No se encontró requirements.txt
)

echo -----------------------------------------
echo ✅ Entorno listo para trabajar ✔️
pause
