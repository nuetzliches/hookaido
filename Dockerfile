# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.25-alpine AS build
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
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -h /app hookaido
WORKDIR /app
COPY --from=build /hookaido /usr/local/bin/hookaido
USER hookaido
EXPOSE 8080 9443 2019
VOLUME ["/app/.data"]
ENTRYPOINT ["hookaido"]
CMD ["run", "--config", "/app/Hookaidofile", "--db", "/app/.data/hookaido.db"]
