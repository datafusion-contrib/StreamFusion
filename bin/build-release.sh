#!/usr/bin/env sh

set -eu

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "--host-only" ]; }; then
  echo "usage: $0 [--host-only]" >&2
  exit 64
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
native_dir=$repo_root/native
stage_dir=$native_dir/target/universal
host_only=false

if [ "$#" -eq 1 ]; then
  host_only=true
fi

host_platform() {
  case "$(uname -s)" in
    Linux) printf '%s\n' linux ;;
    Darwin) printf '%s\n' darwin ;;
    *) echo "unsupported host operating system: $(uname -s)" >&2; exit 69 ;;
  esac
}

host_architecture() {
  case "$(uname -m)" in
    x86_64|amd64) printf '%s\n' x86_64 ;;
    arm64|aarch64) printf '%s\n' aarch64 ;;
    *) echo "unsupported host architecture: $(uname -m)" >&2; exit 69 ;;
  esac
}

stage_host_library() {
  extension=$1
  features=$2
  platform=$(host_platform)
  architecture=$(host_architecture)
  case "$platform" in
    linux) library=libstreamfusion.so ;;
    darwin) library=libstreamfusion.dylib ;;
  esac

  (cd "$native_dir" && cargo build --release --no-default-features --features "$features" \
    --target-dir "$native_dir/target/release-staging")
  stage_native_library \
    "$extension" "$platform" "$architecture" \
    "$native_dir/target/release-staging/release/$library"
}

stage_linux_libraries() {
  platform=$1
  architecture=$2
  image=streamfusion-native-release-$architecture
  container=

  cleanup_container() {
    if [ -n "$container" ]; then
      docker rm --force "$container" >/dev/null 2>&1 || true
    fi
  }

  trap cleanup_container EXIT HUP INT TERM
  docker build --platform "$platform" --tag "$image" \
    --file "$repo_root/docker/native-release.Dockerfile" "$native_dir"
  container=$(docker create --platform "$platform" "$image")
  for extension in core kafka json csv raw avro protobuf fluss parquet; do
    case "$extension" in
      core)
        destination_directory=$stage_dir/linux/$architecture
        destination_library=libstreamfusion.so
        ;;
      *)
        destination_directory=$stage_dir/$extension/linux/$architecture
        destination_library=libstreamfusion_$extension.so
        ;;
    esac
    mkdir -p "$destination_directory"
    docker cp "$container:/workspace/out/$extension/$destination_library" \
      "$destination_directory/$destination_library"
  done
  cleanup_container
  container=
  trap - EXIT HUP INT TERM
}

stage_native_library() {
  extension=$1
  platform=$2
  architecture=$3
  source_library=$4
  case "$extension" in
    core)
      destination_directory=$stage_dir/$platform/$architecture
      destination_library=$(basename "$source_library")
      ;;
    *)
      destination_directory=$stage_dir/$extension/$platform/$architecture
      case "$platform" in
        linux) destination_library=libstreamfusion_$extension.so ;;
        darwin) destination_library=libstreamfusion_$extension.dylib ;;
      esac
      ;;
  esac
  mkdir -p "$destination_directory"
  cp "$source_library" "$destination_directory/$destination_library"
}

rm -rf "$stage_dir"
mkdir -p "$stage_dir"
stage_host_library core mimalloc
# The sink encode dispatch lives in the connector library (JSON is compiled with the kafka
# feature itself); the csv feature adds the CSV encode arm there. The decode-side CSV format
# still ships as its own extension library below.
stage_host_library kafka mimalloc,kafka,csv,avro,protobuf
stage_host_library json mimalloc,json
stage_host_library csv mimalloc,csv
stage_host_library raw mimalloc,raw
stage_host_library avro mimalloc,avro
stage_host_library protobuf mimalloc,protobuf
stage_host_library fluss mimalloc,fluss
stage_host_library parquet mimalloc,parquet

if [ "$host_only" = false ]; then
  command -v docker >/dev/null 2>&1 || {
    echo "Docker is required to build the Linux release libraries." >&2
    exit 69
  }
  stage_linux_libraries linux/amd64 x86_64
  stage_linux_libraries linux/arm64 aarch64
fi

(cd "$repo_root" && mvn package -Pdist,universal -DskipTests)
