#!/usr/bin/env sh

set -eu

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "--host-only" ]; }; then
  echo "usage: $0 [--host-only]" >&2
  exit 64
fi

host_only=false
if [ "$#" -eq 1 ]; then
  host_only=true
  case "$(uname -s)" in
    Linux) host_platform=linux; host_extension=so ;;
    Darwin) host_platform=darwin; host_extension=dylib ;;
    *) echo "unsupported host operating system: $(uname -s)" >&2; exit 69 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) host_architecture=x86_64 ;;
    arm64|aarch64) host_architecture=aarch64 ;;
    *) echo "unsupported host architecture: $(uname -m)" >&2; exit 69 ;;
  esac
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
modules="core kafka json csv raw avro avro-confluent-registry protobuf parquet"
entries=$(mktemp)
native_entries=$(mktemp)
expected_native_entries=$(mktemp)
trap 'rm -f "$entries" "$native_entries" "$expected_native_entries"' EXIT HUP INT TERM

assert_native_payload() {
  jar_file=$1
  module=$2
  library=$3
  resource_directory=$4

  jar tf "$jar_file" | awk '/^tech\/streamfusion\/native\/.*\.(so|dylib)$/ { print }' \
    | sort >"$native_entries"
  if [ -n "$resource_directory" ]; then
    resource_directory="$resource_directory/"
  fi
  if [ "$host_only" = true ]; then
    echo "tech/streamfusion/native/${resource_directory}$host_platform/$host_architecture/$library.$host_extension" \
      >"$expected_native_entries"
  else
    {
      echo "tech/streamfusion/native/${resource_directory}linux/x86_64/$library.so"
      echo "tech/streamfusion/native/${resource_directory}darwin/aarch64/$library.dylib"
    } | sort >"$expected_native_entries"
  fi
  if ! cmp -s "$expected_native_entries" "$native_entries"; then
    echo "$module does not contain the exact supported native release matrix:" >&2
    diff -u "$expected_native_entries" "$native_entries" >&2 || true
    exit 1
  fi
}

assert_no_native_payload() {
  jar_file=$1
  module=$2
  if jar tf "$jar_file" | grep -Eq '^tech/streamfusion/native/.*\.(so|dylib)$'; then
    echo "$module unexpectedly contains a loose native library" >&2
    exit 1
  fi
}

for suffix in $modules; do
  module="streamfusion-$suffix"
  if [ "$suffix" = core ]; then
    jar_file="$repo_root/$module/target/$module-$version-runtime.jar"
  else
    jar_file="$repo_root/$module/target/$module-$version.jar"
  fi
  if [ ! -f "$jar_file" ]; then
    echo "missing artifact: $jar_file" >&2
    exit 1
  fi
  jar tf "$jar_file" | awk -v module="$module" \
    '/^tech\/streamfusion\/.*\.class$/ { print $0, module }' >>"$entries"
  if [ "$suffix" != core ] && jar tf "$jar_file" \
      | grep -Eq '^tech/streamfusion/native/libstreamfusion\.(so|dylib)$'; then
    echo "$module contains the core development DSO" >&2
    exit 1
  fi
done

core_jar="$repo_root/streamfusion-core/target/streamfusion-core-$version-runtime.jar"
core_main_jar="$repo_root/streamfusion-core/target/streamfusion-core-$version.jar"
assert_native_payload "$core_main_jar" streamfusion-core libstreamfusion ""
assert_native_payload "$core_jar" streamfusion-core libstreamfusion ""
if jar tf "$core_jar" | grep -Eq '^tech/streamfusion/(kafka|parquet|format/(json|csv|raw|avro|avroconfluent|protobuf))/'; then
  echo "streamfusion-core contains optional connector or format classes" >&2
  exit 1
fi

for suffix in kafka json csv raw avro protobuf parquet; do
  assert_native_payload \
    "$repo_root/streamfusion-$suffix/target/streamfusion-$suffix-$version.jar" \
    "streamfusion-$suffix" "libstreamfusion_$suffix" "$suffix"
done

assert_no_native_payload \
  "$repo_root/streamfusion-runtime/target/streamfusion-runtime-$version.jar" \
  streamfusion-runtime
assert_no_native_payload \
  "$repo_root/streamfusion-avro-confluent-registry/target/streamfusion-avro-confluent-registry-$version.jar" \
  streamfusion-avro-confluent-registry

loader_jar="$repo_root/streamfusion-loader/target/streamfusion-loader-$version.jar"
if [ ! -f "$loader_jar" ]; then
  echo "missing artifact: $loader_jar" >&2
  exit 1
fi
assert_no_native_payload "$loader_jar" streamfusion-loader
if ! unzip -p "$loader_jar" streamfusion-planner.jar | cmp -s - "$core_jar"; then
  echo "streamfusion-loader does not embed the exact core runtime payload" >&2
  exit 1
fi

duplicates=$(sort "$entries" | awk 'previous == $1 { print $1 } { previous = $1 }' | sort -u)
if [ -n "$duplicates" ]; then
  echo "StreamFusion classes occur in more than one deployable artifact:" >&2
  echo "$duplicates" >&2
  exit 1
fi

confluent_jar="$repo_root/streamfusion-avro-confluent-registry/target/streamfusion-avro-confluent-registry-$version.jar"
if jar tf "$confluent_jar" | grep -q 'libstreamfusion_avro'; then
  echo "the Confluent integration duplicates streamfusion-avro's native library" >&2
  exit 1
fi

echo "StreamFusion artifact boundaries are clean for $version"
