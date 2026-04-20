# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.26-alpine@sha256:f85330846cde1e57ca9ec309382da3b8e6ae3ab943d2739500e08c86393a21b1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/nuetzliches/hookaido/internal/app.version=${VERSION} -X github.com/nuetzliches/hookaido/internal/app.commit=${COMMIT} -X github.com/nuetzliches/hookaido/internal/app.buildDate=${BUILD_DATE}" \
    -o /hookaido ./cmd/hookaido

# --- Runtime stage ---
FROM alpine:3.23@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659
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
