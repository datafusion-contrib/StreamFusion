ARG FLINK_IMAGE=flink:2.2.1-scala_2.12-java17
ARG STREAMFUSION_VERSION=0.1.0-rc3
FROM ${FLINK_IMAGE}

ARG FLINK_IMAGE
ARG STREAMFUSION_VERSION
ARG STREAMFUSION_ARTIFACT_SUFFIX=""
ARG FLINK_LINE=2.2

LABEL org.opencontainers.image.title="StreamFusion Flink base image" \
      org.opencontainers.image.description="Flink ${FLINK_LINE} with StreamFusion's native planner and runtime" \
      tech.streamfusion.flink-base-image="${FLINK_IMAGE}"

# The release library links mimalloc inside its own DSO. Reserve enough static TLS before the JVM
# starts so glibc can load that library safely from Flink task threads without a process-wide
# allocator override. The stock glibc default is 512 bytes; an optimized native DSO needs just
# under 16 KiB. Reserve 128 KiB per thread so the core plus several optional extensions can be
# loaded safely from Flink task threads without a process-wide allocator override.
# The official Flink image is glibc-based. Tell RocksDBJNI directly so its Java fallback backend
# does not spawn an `ldd` probe from a task thread during operator initialization.
ENV GLIBC_TUNABLES=glibc.rtld.optional_static_tls=131072 \
    ROCKSDB_MUSL_LIBC=false

# These are Flink runtime extensions, not user-job dependencies. Keep the loader first so its
# PlannerModule shadow is resolved before Flink's stock planner loader.
COPY streamfusion-loader/target/streamfusion-loader${STREAMFUSION_ARTIFACT_SUFFIX}-${STREAMFUSION_VERSION}.jar \
     /opt/flink/lib/00-streamfusion-loader.jar
COPY streamfusion-core/target/streamfusion-core${STREAMFUSION_ARTIFACT_SUFFIX}-${STREAMFUSION_VERSION}-runtime.jar \
     /opt/flink/lib/streamfusion-core.jar

COPY --chmod=755 docker/streamfusion-entrypoint.sh /streamfusion-entrypoint.sh
ENTRYPOINT ["/streamfusion-entrypoint.sh"]
CMD ["help"]
