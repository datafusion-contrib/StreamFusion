FROM rust:1.94-bookworm

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential pkg-config protobuf-compiler perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace/native

WORKDIR /workspace/native
RUN set -eux; \
    cargo build --release --no-default-features --features mimalloc; \
    mkdir -p /workspace/out/core; \
    cp target/release/libstreamfusion.so /workspace/out/core/libstreamfusion.so; \
    cargo build --release --no-default-features --features mimalloc,kafka,csv,avro,protobuf,raw; \
    mkdir -p /workspace/out/kafka; \
    cp target/release/libstreamfusion.so /workspace/out/kafka/libstreamfusion_kafka.so; \
    cargo build --release --no-default-features --features mimalloc,json; \
    mkdir -p /workspace/out/json; \
    cp target/release/libstreamfusion.so /workspace/out/json/libstreamfusion_json.so; \
    cargo build --release --no-default-features --features mimalloc,csv; \
    mkdir -p /workspace/out/csv; \
    cp target/release/libstreamfusion.so /workspace/out/csv/libstreamfusion_csv.so; \
    cargo build --release --no-default-features --features mimalloc,raw; \
    mkdir -p /workspace/out/raw; \
    cp target/release/libstreamfusion.so /workspace/out/raw/libstreamfusion_raw.so; \
    cargo build --release --no-default-features --features mimalloc,avro; \
    mkdir -p /workspace/out/avro; \
    cp target/release/libstreamfusion.so /workspace/out/avro/libstreamfusion_avro.so; \
    cargo build --release --no-default-features --features mimalloc,protobuf; \
    mkdir -p /workspace/out/protobuf; \
    cp target/release/libstreamfusion.so /workspace/out/protobuf/libstreamfusion_protobuf.so; \
    cargo build --release --no-default-features --features mimalloc,fluss; \
    mkdir -p /workspace/out/fluss; \
    cp target/release/libstreamfusion.so /workspace/out/fluss/libstreamfusion_fluss.so; \
    cargo build --release --no-default-features --features mimalloc,parquet; \
    mkdir -p /workspace/out/parquet; \
    cp target/release/libstreamfusion.so /workspace/out/parquet/libstreamfusion_parquet.so
