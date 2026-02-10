# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.25-alpine@sha256:f6751d823c26342f9506c03797d2527668d095b0a15f1862cddb4d927a7a4ced AS build
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
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -h /app hookaido
WORKDIR /app
COPY --from=build /hookaido /usr/local/bin/hookaido
USER hookaido
EXPOSE 8080 9443 2019
VOLUME ["/app/.data"]
ENTRYPOINT ["hookaido"]
CMD ["run", "--config", "/app/Hookaidofile", "--db", "/app/.data/hookaido.db"]
