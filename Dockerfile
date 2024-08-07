FROM golang:1.22.5-alpine as build

WORKDIR /go/src/

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /go/bin/redis-inventory

ENTRYPOINT ["/go/bin/redis-inventory"]

FROM alpine:3.20.1 AS dist

WORKDIR /go/bin/

COPY --from=build /go/bin/ .

ENTRYPOINT ["/go/bin/redis-inventory"]
