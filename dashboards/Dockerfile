FROM grafana/grafana:11.6.0

COPY provisioning/ /etc/grafana/provisioning/
COPY *.json /var/lib/grafana/dashboards/

ENV GF_SERVER_HTTP_PORT=8080
ENV GF_AUTH_ANONYMOUS_ENABLED=false
