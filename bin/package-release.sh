#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
output_dir=${1:-"$repo_root/target/release"}
stage_dir=$(mktemp -d)
bundle_dir=$stage_dir/streamfusion-$version

cleanup() {
  rm -rf "$stage_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$bundle_dir" "$output_dir"
cp "$repo_root/LICENSE" "$repo_root/readme.md" "$bundle_dir/"
cp "$repo_root/streamfusion-loader/target/streamfusion-loader-$version.jar" "$bundle_dir/"
cp "$repo_root/streamfusion-core/target/streamfusion-core-$version-runtime.jar" "$bundle_dir/"

for suffix in kafka json csv raw avro avro-confluent-registry protobuf parquet; do
  cp "$repo_root/streamfusion-$suffix/target/streamfusion-$suffix-$version.jar" "$bundle_dir/"
done

archive=$output_dir/streamfusion-$version-bin.tar.gz
(cd "$stage_dir" && tar -czf "$archive" "streamfusion-$version")
(cd "$output_dir" && shasum -a 256 "$(basename "$archive")" > "$(basename "$archive").sha256")

printf '%s\n' "$archive"
