#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SF_PAIMON_ARTIFACTS="${SF_PAIMON_ARTIFACTS:-${REPO_ROOT}/target/paimon-nexmark-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${SF_PAIMON_ARTIFACTS}"
SF_PAIMON_ARTIFACTS="$(cd "${SF_PAIMON_ARTIFACTS}" && pwd)"
export SF_PAIMON_ARTIFACTS
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx512m}"
java_major="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
if [[ "${java_major}" != 17 ]]; then
  echo 'This comparison uses JDK 17; set JAVA_HOME and PATH to a JDK 17 installation.' >&2
  exit 2
fi
packages=(-p streamfusion -p streamfusion-parquet -p streamfusion-orc -p streamfusion-paimon -p streamfusion-kafka -p streamfusion-json)
libraries=(streamfusion streamfusion_parquet streamfusion_orc streamfusion_paimon streamfusion_kafka streamfusion_json)
cargo build --manifest-path "${REPO_ROOT}/native/Cargo.toml" --release --features mimalloc \
  "${packages[@]}" > "${SF_PAIMON_ARTIFACTS}/cargo.log" 2>&1
cargo_target="$(cargo metadata --manifest-path "${REPO_ROOT}/native/Cargo.toml" --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
staged="${SF_PAIMON_ARTIFACTS}/native"
mkdir -p "${staged}"
for library in "${libraries[@]}"; do
  case "$(uname -s)" in Darwin) extension=dylib ;; *) extension=so ;; esac
  cp "${cargo_target}/release/lib${library}.${extension}" "${staged}/"
done
python3 - "${REPO_ROOT}" "${SF_PAIMON_ARTIFACTS}" <<'PY'
import hashlib, json, pathlib, platform, subprocess, sys
repo, output = map(pathlib.Path, sys.argv[1:])
def command(*args): return subprocess.check_output(args, cwd=repo, text=True).strip()
metadata = {
    'commit': command('git', 'rev-parse', 'HEAD'),
    'dirty': bool(command('git', 'status', '--porcelain')),
    'platform': platform.platform(), 'processor': platform.processor(),
    'java': subprocess.run(['java', '-version'], text=True, capture_output=True).stderr.strip(),
    'rust': command('rustc', '--version'),
    'build': 'cargo build --release --features mimalloc -p streamfusion -p streamfusion-parquet -p streamfusion-orc -p streamfusion-paimon -p streamfusion-kafka -p streamfusion-json',
    'javaHeap': __import__('os').environ.get('SF_PAIMON_JAVA_HEAP', '4g'),
    'libraries': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (output / 'native').iterdir()},
}
if pathlib.Path('/proc/cpuinfo').exists():
    metadata['cpu'] = next((line.split(':', 1)[1].strip() for line in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), '')
(output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
PY
export SF_BENCHMARK=true SF_PAIMON_MATCHED=true SF_PAIMON_RELEASE_CONFIRMED=true
export SF_ROWS="${SF_ROWS:-2000000}" SF_WARMUP="${SF_WARMUP:-1}" SF_RUNS="${SF_RUNS:-3}"
export SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4 SF_LAKE_STATE_BACKEND=disk
export SF_BENCH_MANAGED_MEMORY="${SF_BENCH_MANAGED_MEMORY:-512 mb}"
mvn -B -ntp -f "${REPO_ROOT}/pom.xml" -Ppaimon,bench \
  -pl streamfusion-paimon -am -Dtest=NexmarkPaimonMatchedBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true -Dnative.build.skip=true \
  "-Dnative.resource.directory=${staged}" "-Dsf.extraJvmArgs=-Djava.library.path='${staged}' -Xmx${SF_PAIMON_JAVA_HEAP:-4g}" \
  test 2>&1 | tee "${SF_PAIMON_ARTIFACTS}/run.log"
python3 "${REPO_ROOT}/bin/report-paimon.py" "${SF_PAIMON_ARTIFACTS}"
