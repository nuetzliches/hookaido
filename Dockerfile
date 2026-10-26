# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/nuetzliches/hookaido/v2/internal/app.version=${VERSION} -X github.com/nuetzliches/hookaido/v2/internal/app.commit=${COMMIT} -X github.com/nuetzliches/hookaido/v2/internal/app.buildDate=${BUILD_DATE}" \
    -o /hookaido ./cmd/hookaido

# --- Runtime stage ---
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache ca-certificates tzdata su-exec && \
    adduser -D -u 1000 -h /app hookaido
WORKDIR /app
COPY --from=build /hookaido /usr/local/bin/hookaido
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
EXPOSE 8080 9443 2019
VOLUME ["/app/.data"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run", "--config", "/app/Hookaidofile", "--db", "/app/.data/hookaido.db"]
