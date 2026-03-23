# ================================
# Stage 1 – Builder
# ================================
FROM python:3.12-slim AS base

# Evita que Python genere archivos .pyc y activa el buffer sin bloqueos
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1


# Instala dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#Crea el entorno virtual
ENV VENV_PATH=/opt/venv
RUN python -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

WORKDIR /app

# ============================================
# CAPA 2: Dependencias base (cambian más frecuentemente)
# ============================================

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el resto de la aplicación
COPY . /app

#TODO ejecutar tests

WORKDIR /app

# Use exec form and ensure Python runs unbuffered for immediate log output
CMD ["python", "-u", "main.py"]
