FROM rust:latest
WORKDIR /usr/src/mini_redis
COPY . .
RUN cargo build --release
EXPOSE 6379
ENTRYPOINT ["./target/release/mini_redis"]