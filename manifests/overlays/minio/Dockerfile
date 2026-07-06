FROM debian:13-slim

WORKDIR /opt

RUN apt update && \
    apt install -y systemd wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.155.0/otelcol-contrib_0.155.0_linux_amd64.tar.gz" && \
    tar -xzf "otelcol-contrib_0.155.0_linux_amd64.tar.gz"
COPY otelcol-config.yaml /opt/otelcol-config.yaml

ENTRYPOINT ["/opt/otelcol-contrib"]
CMD ["--config", "/opt/otelcol-config.yaml"]
