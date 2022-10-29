FROM golang:alpine as builder

RUN mkdir /build
WORKDIR /build
ADD . /build/

RUN apk add git
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o sequencer .

FROM scratch

COPY --from=builder /build/sequencer /app/
WORKDIR /app
EXPOSE 62081
CMD ["./sequencer"]