FROM rust:1.87-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin substrate

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && useradd --system --uid 10001 --create-home substrate && mkdir /data && chown substrate:substrate /data
COPY --from=build /build/target/release/substrate /usr/local/bin/substrate
USER substrate
VOLUME ["/data"]
EXPOSE 7432
ENTRYPOINT ["substrate"]
CMD ["--data", "/data/substrate.redb", "--listen", "0.0.0.0:7432"]
