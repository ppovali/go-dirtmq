FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o broker-bin cmd/broker/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o client-bin cmd/cli-client/main.go

FROM scratch AS broker

COPY --from=builder /app/broker-bin /dirtmq

EXPOSE 8080

ENTRYPOINT [ "/dirtmq" ]


FROM scratch AS client

COPY --from=builder /app/client-bin /client

ENTRYPOINT [ "/client" ]