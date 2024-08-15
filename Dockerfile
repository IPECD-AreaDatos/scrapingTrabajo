# Usar una imagen base de Python (por ejemplo, Alpine, Debian, etc.)
FROM python:3.9-slim

# Actualizar los repositorios de paquetes y luego instalar sudo y git
RUN apt-get update && apt-get install -y sudo git

# Crear el directorio de trabajo
RUN mkdir -p /app/src

# Establecer el directorio de trabajo
WORKDIR /app/src

# Copiar cualquier archivo necesario (si es necesario)
# COPY . /app/src

# Comando por defecto para ejecutar cuando se inicie el contenedor
CMD ["/bin/sh"]