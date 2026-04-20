# ================================
# Stage 1 – Builder
# ================================
FROM python:3.12-slim AS builder

ARG MONGO_STRING
ENV MONGO_STRING=${MONGO_STRING}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV VENV_PATH=/opt/venv
RUN python -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app in builder (optional but cleaner if you want pre-compiled deps ready)
COPY . /app

# ================================
# Stage 2 – Runtime (Promtail)
# ================================
FROM grafana/promtail:latest AS prod

RUN apt-get update && apt-get install -y python3 && ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Copy python runtime + app
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

ENV PATH="/opt/venv/bin:$PATH"

# VARIABLES AND SECRETS FROM ENVIRONMENT
ARG MONGO_STRING
ARG APP_NAME
ARG PORT

ENV MONGO_STRING=${MONGO_STRING}
ENV APP_NAME=${APP_NAME}
ENV PORT=${PORT}

# Config
COPY ./deploys/promtail-config.yaml /etc/promtail/config.yaml
COPY ./deploys/entrypoint.sh /entrypoint.sh

# Fix line endings + permissions
USER root
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

EXPOSE ${PORT}

ENTRYPOINT ["/entrypoint.sh"]
