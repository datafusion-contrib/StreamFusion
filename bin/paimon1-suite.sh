#!/usr/bin/env bash
# Run released Paimon 1.0.0 SQL tests on Flink 1.18 with the StreamFusion planner.
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"
test_forks=${SF_PAIMON1_TEST_FORKS:-1}
if [[ ! "$test_forks" =~ ^[1-9][0-9]*$ ]]; then
  echo 'SF_PAIMON1_TEST_FORKS must be a positive integer.' >&2
  exit 64
fi
work_dir="$repo_dir/streamfusion-paimon1/target/upstream-suite"
mkdir -p "$work_dir"
rm -rf "$work_dir/inventory" "$work_dir/diagnostics"
mkdir -p "$work_dir/diagnostics"
archive="$work_dir/paimon-release-1.0.0.tar.gz"
if [[ ! -f "$archive" ]]; then
  curl --fail --location --retry 3 \
    https://codeload.github.com/apache/paimon/tar.gz/refs/tags/release-1.0.0 \
    --output "$archive"
fi
python3 - "$archive" "$work_dir" <<'PY'
import hashlib, pathlib, sys, tarfile
archive, output = map(pathlib.Path, sys.argv[1:])
expected = 'b3d245ccbfbe3c33dcc9772ebaf69b910885e726d7f937af5b606fbb645e3a3c'
if hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
    raise SystemExit('Paimon 1.0.0 source archive checksum mismatch')
# Only the two SQL tests specific to Flink 1.18 are compiled from source.
# Production code and all common tests come from canonical released Maven artifacts.
with tarfile.open(archive) as source:
    for name in ('ContinuousFileStoreITCase', 'UnawareBucketAppendOnlyTableITCase'):
        relative = 'org/apache/paimon/flink/' + name + '.java'
        member = 'paimon-release-1.0.0/paimon-flink/paimon-flink-1.18/src/test/java/' + relative
        target = output / 'src' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.extractfile(member).read())
PY
mvn -B -ntp -f dev/flink-suite/agent/pom.xml \
  -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -DskipTests package
agent="$repo_dir/dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
tests=${SF_PAIMON1_TESTS:-AppendOnlyTableITCase,FirstRowITCase,DynamicBucketTableITCase,ReadWriteTableITCase,PrimaryKeyFileStoreTableITCase,PartialUpdateITCase,FullCompactionFileStoreITCase,FlinkJobRecoveryITCase,ContinuousFileStoreITCase,UnawareBucketAppendOnlyTableITCase,SchemaChangeITCase,FilterPushdownWithSchemaChangeITCase,LookupChangelogWithAggITCase,PreAggregationITCase*,CompositePkAndMultiPartitionedTableITCase}
jvm_args="-Xmx${SF_PAIMON1_TEST_HEAP:-2g} -XX:ActiveProcessorCount=${SF_PAIMON1_TEST_CPUS:-2} --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
jvm_args="$jvm_args -Dlog4j.configurationFile=\"$repo_dir/dev/flink-suite/paimon-log4j2.properties\""
# Keep the placeholder literal until Surefire assigns each fork its identity.
jvm_args+=' -Dmvn.forkNumber=${surefire.forkNumber}'
if [[ ${SF_PAIMON1_STOCK:-false} != true ]]; then
  jvm_args="$jvm_args -javaagent:\"$agent\" -Dstreamfusion.logFallbackReasons=true"
fi
# Bound build and test concurrency independently of the host's CPU count.
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
mvn -B -ntp -Pflink-1.18,paimon1,upstream-paimon1 \
  -pl streamfusion-paimon1 -am -Dsf.testForks="$test_forks" \
  -Dtest="$tests" -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs="$jvm_args" "$@" test 2>&1 | tee "$work_dir/run.log"
if [[ ${SF_PAIMON1_STOCK:-false} != true && -z ${SF_PAIMON1_TESTS:-} ]]; then
  if ! grep -Eq 'wrote a native Paimon (bundle|level-0 file)' "$work_dir/run.log"; then
    echo 'No native Paimon sink execution was observed in the selected upstream suite.' >&2
    exit 1
  fi
fi
