# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
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
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
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
