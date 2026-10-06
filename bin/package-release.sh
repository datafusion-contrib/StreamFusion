#!/usr/bin/env sh

set -eu

flink_line=2.2
output_dir=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --flink-line)
      if [ "$#" -lt 2 ]; then echo "--flink-line requires 2.2 or 1.18" >&2; exit 64; fi
      flink_line=$2; shift 2 ;;
    --*) echo "usage: $0 [--flink-line 2.2|1.18] [output-directory]" >&2; exit 64 ;;
    *)
      if [ -n "$output_dir" ]; then echo "only one output directory is allowed" >&2; exit 64; fi
      output_dir=$1; shift ;;
  esac
done
artifact_suffix=""
modules="kafka json csv raw avro avro-confluent-registry protobuf parquet orc paimon"
case "$flink_line" in
  2.2) set --; modules="$modules delta" ;;
  1.18) artifact_suffix=-flink1.18; set -- -Pflink-1.18 ;;
  *) echo "unsupported Flink line: $flink_line" >&2; exit 64 ;;
esac

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
version=$(cd "$repo_root" && mvn "$@" -q -DforceStdout help:evaluate -Dexpression=project.version)
output_dir=${output_dir:-"$repo_root/target/release"}
bundle_name=streamfusion${artifact_suffix}-$version
stage_dir=$(mktemp -d)
bundle_dir=$stage_dir/$bundle_name

cleanup() {
  rm -rf "$stage_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$bundle_dir" "$output_dir"
output_dir=$(CDPATH='' cd -- "$output_dir" && pwd)
cp "$repo_root/LICENSE" "$repo_root/readme.md" "$bundle_dir/"
cp "$repo_root/streamfusion-loader/target/streamfusion-loader${artifact_suffix}-$version.jar" "$bundle_dir/"
cp "$repo_root/streamfusion-core/target/streamfusion-core${artifact_suffix}-$version-runtime.jar" "$bundle_dir/"

for suffix in $modules; do
  cp "$repo_root/streamfusion-$suffix/target/streamfusion-$suffix${artifact_suffix}-$version.jar" "$bundle_dir/"
done

archive=$output_dir/$bundle_name-bin.tar.gz
(cd "$stage_dir" && COPYFILE_DISABLE=1 tar -czf "$archive" "$bundle_name")
(cd "$output_dir" && shasum -a 256 "$(basename "$archive")" > "$(basename "$archive").sha256")

printf '%s\n' "$archive"
