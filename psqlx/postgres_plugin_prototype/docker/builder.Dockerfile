ARG PG_MAJOR=15
FROM rust:1.86-bookworm

ARG PG_MAJOR
ARG CARGO_PGRX_VERSION=0.11.4

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        libclang-dev \
        pkg-config \
        postgresql-$PG_MAJOR \
        postgresql-client-$PG_MAJOR \
        postgresql-server-dev-$PG_MAJOR \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --locked cargo-pgrx --version ${CARGO_PGRX_VERSION}

ENV PATH=/root/.cargo/bin:$PATH
WORKDIR /workspace