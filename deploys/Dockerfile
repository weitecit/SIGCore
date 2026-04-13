# ================================
# Stage 1 – Builder
# ================================
FROM python:3.12-slim AS builder

# VARIABLES AND SECRETS FROM ENVIRONMENT
ARG MONGO_STRING

ENV MONGO_STRING=${MONGO_STRING}
	
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

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# ================================
# Stage 2 – Runtime
# ================================
FROM python:3.12-slim
ENV PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv
COPY . /app

WORKDIR /app


# Use exec form and ensure Python runs unbuffered for immediate log output
CMD ["python", "-u", "main.py"]
