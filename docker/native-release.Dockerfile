# Keep the libc and C++ symbol floor below the official Flink 1.18 image (Ubuntu 22.04).
FROM rust:1.94-bullseye

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential clang libclang-dev pkg-config protobuf-compiler perl cmake python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace/native

WORKDIR /workspace/native
# Build distinct packages together so dependencies are compiled once for the release.
RUN cargo build --release --workspace --features mimalloc \
    && mkdir -p /workspace/out/core \
    && cp target/release/libstreamfusion.so /workspace/out/core/libstreamfusion.so \
    && for extension in kafka fluss json csv raw avro protobuf parquet orc paimon; do \
         mkdir -p "/workspace/out/$extension"; \
         library="target/release/libstreamfusion_$extension.so"; \
         if nm -D --defined-only "$library" | grep -q ' Java_tech_streamfusion_Native_'; then exit 70; fi; \
         cp "$library" "/workspace/out/$extension/"; \
       done
