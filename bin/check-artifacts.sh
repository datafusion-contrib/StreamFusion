#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
modules="core kafka json csv raw avro avro-confluent-registry protobuf fluss parquet"
entries=$(mktemp)
trap 'rm -f "$entries"' EXIT HUP INT TERM

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
if jar tf "$core_jar" | grep -Eq '^tech/streamfusion/(kafka|fluss|parquet|format/(json|csv|raw|avro|avroconfluent|protobuf))/'; then
  echo "streamfusion-core contains optional connector or format classes" >&2
  exit 1
fi

loader_jar="$repo_root/streamfusion-loader/target/streamfusion-loader-$version.jar"
if [ ! -f "$loader_jar" ] \
    || ! unzip -p "$loader_jar" streamfusion-planner.jar | cmp -s - "$core_jar"; then
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
