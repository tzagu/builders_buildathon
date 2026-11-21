# Shopping Sessions: a native Rust pipeline.
# Build stage compiles from source (network is allowed at build time only).
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: AVX2 (x86-64-v3) is safe there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && cp target/release/sessions /sessions

FROM debian:bookworm-slim
COPY --from=build /sessions /app/sessions
# Runs as uid 1000 on a read-only root; it only writes to $OUTPUT_DIR.
CMD ["/app/sessions"]
