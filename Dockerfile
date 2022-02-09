FROM golang:1.17-alpine AS build

RUN apk add --no-cache git

WORKDIR /app/src

# Copy sources
COPY internals/ internals/
COPY cmd/ cmd/
COPY go.mod ./
COPY go.sum ./

# Build module
RUN go build -o /app/build/objectstore-restapi github.com/flaviopicci/simple-objectstore-restapi/cmd/objectstore-restapi

FROM alpine as bin
STOPSIGNAL SIGINT

WORKDIR /app
COPY --from=build /app/build ./

ENTRYPOINT ["./objectstore-restapi"]
