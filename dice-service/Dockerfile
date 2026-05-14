# ─────────────────────────────────────────────
# Stage 1: Build
# ─────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app

# Cache dependencies separately from source
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ─────────────────────────────────────────────
# Stage 2: Download OTel Java Agent
# ─────────────────────────────────────────────
FROM eclipse-temurin:21-jre AS agent-downloader
# Update OTEL_AGENT_VERSION to the latest stable release when rebuilding
ARG OTEL_AGENT_VERSION=2.10.0
RUN apt-get update -qq && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSfL \
       "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar" \
       -o /opentelemetry-javaagent.jar

# ─────────────────────────────────────────────
# Stage 3: Runtime
# ─────────────────────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /app

RUN apt-get update -qq && apt-get install -y --no-install-recommends bash curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=agent-downloader /opentelemetry-javaagent.jar opentelemetry-javaagent.jar
COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8080

# OTel environment variables can be overridden at runtime via K8s env:
ENV OTEL_SERVICE_NAME=dice-service \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector-service:4318 \
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
    OTEL_LOGS_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=none \
    OTEL_TRACES_EXPORTER=otlp

ENTRYPOINT ["java", \
    "-javaagent:opentelemetry-javaagent.jar", \
    "-jar", "app.jar"]
