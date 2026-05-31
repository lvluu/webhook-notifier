FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /build/notifier ./cmd/notifier && \
    go build -o /build/chaos ./cmd/chaos && \
    go build -o /build/webhook-receiver ./cmd/webhook-receiver

FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl

COPY --from=builder /build/notifier /usr/local/bin/
COPY --from=builder /build/chaos /usr/local/bin/
COPY --from=builder /build/webhook-receiver /usr/local/bin/

COPY migrations/ /migrations/

RUN adduser -D -h /home/app app
USER app

CMD ["notifier"]
