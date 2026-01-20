FROM lukemathwalker/cargo-chef:0.1.72-rust-1.89.0-slim AS chef

# Note: Checksum verification omitted as DuckDB does not publish official checksums; relying on HTTPS for transport security
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    build-essential \
    wget \
    unzip \
    && wget -q https://github.com/duckdb/duckdb/releases/download/v1.4.3/libduckdb-linux-amd64.zip \
    && unzip libduckdb-linux-amd64.zip -d /tmp/duckdb \
    && mv /tmp/duckdb/libduckdb.so /usr/local/lib/ \
    && mv /tmp/duckdb/*.h /usr/local/include/ \
    && rm -rf /tmp/duckdb libduckdb-linux-amd64.zip \
    && ldconfig \
    && apt-get purge -y wget unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN rustup toolchain install stable

RUN rustup default stable

ENV LIBRARY_PATH=/usr/local/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH

WORKDIR /app

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./

COPY applications/datamanager/Cargo.toml ./applications/datamanager/Cargo.toml

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./

COPY applications/datamanager/Cargo.toml ./applications/datamanager/Cargo.toml

COPY applications/datamanager/src/ applications/datamanager/src/

ENV DUCKDB_LIB_DIR=/usr/local/lib
ENV DUCKDB_INCLUDE_DIR=/usr/local/include
ENV LIBRARY_PATH=/usr/local/lib${LIBRARY_PATH:+:$LIBRARY_PATH}
ENV LD_LIBRARY_PATH=/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin datamanager && \
    cp /app/target/release/datamanager /tmp/datamanager

FROM debian:trixie-slim AS server

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=chef /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so

RUN ldconfig

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENV SSL_CERT_DIR=/etc/ssl/certs

WORKDIR /app

COPY --from=builder /tmp/datamanager /usr/local/bin/datamanager

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/datamanager"]

