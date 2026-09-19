#!/usr/bin/env sh

set -eu

host_only=false
flink_line=2.2
while [ "$#" -gt 0 ]; do
  case "$1" in
    --host-only) host_only=true; shift ;;
    --flink-line)
      if [ "$#" -lt 2 ]; then echo "--flink-line requires 2.2 or 1.18" >&2; exit 64; fi
      flink_line=$2; shift 2 ;;
    *) echo "usage: $0 [--host-only] [--flink-line 2.2|1.18]" >&2; exit 64 ;;
  esac
done
artifact_suffix=""
case "$flink_line" in
  2.2) set -- ;;
  1.18) artifact_suffix=-flink1.18; set -- -Pflink-1.18 ;;
  *) echo "unsupported Flink line: $flink_line" >&2; exit 64 ;;
esac

if [ "$host_only" = true ]; then
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
version=$(cd "$repo_root" && mvn "$@" -q -DforceStdout help:evaluate -Dexpression=project.version)
modules="core kafka json csv raw avro avro-confluent-registry protobuf parquet orc paimon"
if [ "$flink_line" = 2.2 ]; then modules="$modules delta"; fi
entries=$(mktemp)
native_entries=$(mktemp)
expected_native_entries=$(mktemp)
trap 'rm -f "$entries" "$native_entries" "$expected_native_entries"' EXIT HUP INT TERM

assert_flink_identity() {
  jar_file=$1
  module=$2
  manifest=$(unzip -p "$jar_file" META-INF/MANIFEST.MF | tr -d '\r')
  actual_line=$(printf '%s\n' "$manifest" | sed -n 's/^StreamFusion-Flink-Line: //p')
  actual_module=$(printf '%s\n' "$manifest" | sed -n 's/^StreamFusion-Module: //p')
  if [ "$actual_line" != "$flink_line" ] || [ "$actual_module" != "$module" ]; then
    echo "$jar_file has identity '$actual_module' / Flink '$actual_line', expected '$module' / Flink '$flink_line'" >&2
    exit 1
  fi
}

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
  if [ "$(uname -s)" = Linux ]; then
    python3 "$script_dir/check-native-glibc.py" "$jar_file"
    python3 "$script_dir/check-native-tls.py" "$jar_file"
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
  artifact="${module}${artifact_suffix}"
  if [ "$suffix" = core ]; then
    jar_file="$repo_root/$module/target/$artifact-$version-runtime.jar"
  else
    jar_file="$repo_root/$module/target/$artifact-$version.jar"
  fi
  if [ ! -f "$jar_file" ]; then
    echo "missing artifact: $jar_file" >&2
    exit 1
  fi
  assert_flink_identity "$jar_file" "$artifact"
  if jar tf "$jar_file" | grep -q '^org/slf4j/'; then
    echo "$artifact must use Flink's logging API instead of bundling SLF4J" >&2
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

core_jar="$repo_root/streamfusion-core/target/streamfusion-core${artifact_suffix}-$version-runtime.jar"
core_main_jar="$repo_root/streamfusion-core/target/streamfusion-core${artifact_suffix}-$version.jar"
assert_flink_identity "$core_main_jar" "streamfusion-core${artifact_suffix}"
assert_native_payload "$core_main_jar" streamfusion-core libstreamfusion ""
assert_native_payload "$core_jar" streamfusion-core libstreamfusion ""
if jar tf "$core_jar" | grep -Eq '^tech/streamfusion/(kafka|parquet|orc|delta|paimon|format/(json|csv|raw|avro|avroconfluent|protobuf))/'; then
  echo "streamfusion-core contains optional connector or format classes" >&2
  exit 1
fi

for suffix in kafka json csv raw avro protobuf parquet orc paimon; do
  assert_native_payload \
    "$repo_root/streamfusion-$suffix/target/streamfusion-$suffix${artifact_suffix}-$version.jar" \
    "streamfusion-$suffix" "libstreamfusion_$suffix" "$suffix"
done

assert_no_native_payload \
  "$repo_root/streamfusion-runtime/target/streamfusion-runtime${artifact_suffix}-$version.jar" \
  streamfusion-runtime
assert_flink_identity \
  "$repo_root/streamfusion-runtime/target/streamfusion-runtime${artifact_suffix}-$version.jar" \
  "streamfusion-runtime${artifact_suffix}"
assert_no_native_payload \
  "$repo_root/streamfusion-avro-confluent-registry/target/streamfusion-avro-confluent-registry${artifact_suffix}-$version.jar" \
  streamfusion-avro-confluent-registry
if [ "$flink_line" = 2.2 ]; then
  assert_no_native_payload \
    "$repo_root/streamfusion-delta/target/streamfusion-delta-$version.jar" streamfusion-delta
elif [ -f "$repo_root/streamfusion-delta/target/streamfusion-delta${artifact_suffix}-$version.jar" ]; then
  echo "Delta has no verified Flink 1.18 deployment artifact" >&2
  exit 1
fi

loader_jar="$repo_root/streamfusion-loader/target/streamfusion-loader${artifact_suffix}-$version.jar"
if [ ! -f "$loader_jar" ]; then
  echo "missing artifact: $loader_jar" >&2
  exit 1
fi
assert_no_native_payload "$loader_jar" streamfusion-loader
assert_flink_identity "$loader_jar" "streamfusion-loader${artifact_suffix}"
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

confluent_jar="$repo_root/streamfusion-avro-confluent-registry/target/streamfusion-avro-confluent-registry${artifact_suffix}-$version.jar"
if jar tf "$confluent_jar" | grep -q 'libstreamfusion_avro'; then
  echo "the Confluent integration duplicates streamfusion-avro's native library" >&2
  exit 1
fi

python3 - "$repo_root" "$version" "$flink_line" "$artifact_suffix" "$modules" <<'PYTHON'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

root, version, line, suffix, modules = sys.argv[1:]
namespace = {"p": "http://maven.apache.org/POM/4.0.0"}
for module in [*modules.split(), "runtime", "loader"]:
    path = Path(root) / f"streamfusion-{module}" / ".flattened-pom.xml"
    pom = ET.parse(path).getroot()
    def value(element, name):
        return element.findtext(f"p:{name}", namespaces=namespace)
    identity = (value(pom, "groupId"), value(pom, "artifactId"), value(pom, "version"))
    expected = ("tech.streamfusion", f"streamfusion-{module}{suffix}", version)
    if identity != expected or "${" in path.read_text():
        raise SystemExit(f"{path}: unresolved or wrong artifact identity {identity}; expected {expected}")
    dependencies = {
        (value(dep, "groupId"), value(dep, "artifactId")): dep
        for dep in pom.findall("p:dependencies/p:dependency", namespace)
    }
    if module != "loader":
        for artifact, scope in [("arrow-vector", "compile"), ("arrow-c-data", "compile"),
                                ("arrow-memory-unsafe", "runtime")]:
            dep = dependencies.get(("org.apache.arrow", artifact))
            if dep is None or (value(dep, "scope") or "compile") != scope:
                raise SystemExit(f"{path}: missing inherited {artifact} dependency in {scope} scope")
    for (group, artifact), dep in dependencies.items():
        dep_version = value(dep, "version")
        if group == "tech.streamfusion" and artifact.startswith("streamfusion-"):
            actual_suffix = "-flink1.18" if artifact.endswith("-flink1.18") else ""
            if actual_suffix != suffix or dep_version != version:
                raise SystemExit(f"{path}: mismatched StreamFusion dependency {artifact}:{dep_version}")
        if group == "org.apache.flink" and not artifact.startswith("flink-shaded-"):
            expected_version = line + "."
            if artifact == "flink-connector-kafka":
                if not dep_version.endswith("-" + line):
                    raise SystemExit(f"{path}: mismatched Kafka connector line {dep_version}")
            elif not dep_version.startswith(expected_version):
                raise SystemExit(f"{path}: mismatched Flink dependency {artifact}:{dep_version}")
PYTHON

echo "StreamFusion artifact boundaries and consumer POMs are clean for $version / Flink $flink_line"
