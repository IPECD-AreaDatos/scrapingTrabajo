#!/bin/bash

echo "🔧 Iniciando configuración del entorno virtual..."

# Crear entorno si no existe
if [ ! -d "venv" ]; then
  echo "📦 Creando entorno virtual en ./venv ..."
  python3 -m venv venv
else
  echo "✅ Entorno virtual ya existe"
fi

# Activar entorno
echo "⚙️ Activando entorno virtual..."
source venv/bin/activate

# Instalar dependencias
if [ -f "requirements.txt" ]; then
  echo "📥 Instalando paquetes desde requirements.txt ..."
  pip install -r requirements.txt
else
  echo "⚠️ No se encontró requirements.txt, instalá manualmente con pip"
fi

echo "✅ Entorno listo para trabajar ✔️"
