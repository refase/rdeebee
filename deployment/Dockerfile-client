FROM rust:latest as builder

WORKDIR /rdb-client
COPY . .

RUN cargo build --release

FROM scratch
COPY --from=builder /rdb-client/target/release/rdb-client /client/

WORKDIR /client
EXPOSE 2048
CMD ["./rdb-client"]