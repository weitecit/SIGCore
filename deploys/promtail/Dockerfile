FROM grafana/promtail:latest

COPY ./deploys/promtail/promtail-config.yaml /etc/promtail/config.yaml

CMD ["-config.file=/etc/promtail/config.yaml"]

