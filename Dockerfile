# ============================================================
# Dockerfile — scrapingTrabajo
# Build:   docker build -t scraping-trabajo .
# Run:     docker run --env-file .env scraping-trabajo python automaticos/scrap_SIPA/main.py
# ============================================================

FROM python:3.11-slim

# Evitar prompts interactivos durante apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Instalar Chrome y dependencias del sistema
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias Python
# selenium>=4.6 incluye selenium-manager que descarga chromedriver automáticamente
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el proyecto
COPY . .

# Crear carpeta de logs
RUN mkdir -p logs

# Python sin buffering (para que los logs aparezcan en tiempo real)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Sin CMD fijo — cada scraper se ejecuta individualmente
# Ejemplo: docker run --env-file .env scraping-trabajo python automaticos/scrap_SIPA/main.py