#!/bin/sh

set -e

# Crear el archivo de log
touch /tmp/app.log

# Iniciar Promtail en segundo plano
echo "▶ Starting Promtail..."
promtail -config.file=/etc/promtail/config.yaml &

PROMTAIL_PID=$!

# Iniciar la aplicación y redirigir logs
echo "▶ Starting Go API..."
python3 main.py 2>&1 | tee -a /tmp/app.log &

APP_PID=$!

# Esperar a que ambos procesos terminen
wait $APP_PID $PROMTAIL_PID

