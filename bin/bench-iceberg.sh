#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SF_ICEBERG_ARTIFACTS="${SF_ICEBERG_ARTIFACTS:-${REPO_ROOT}/target/iceberg-nexmark-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${SF_ICEBERG_ARTIFACTS}"
SF_ICEBERG_ARTIFACTS="$(cd "${SF_ICEBERG_ARTIFACTS}" && pwd)"
export SF_ICEBERG_ARTIFACTS
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx512m}"
java_major="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
if [[ "${java_major}" != 17 ]]; then
  echo 'This comparison uses JDK 17; set JAVA_HOME and PATH to a JDK 17 installation.' >&2
  exit 2
fi
packages=(-p streamfusion -p streamfusion-iceberg)
libraries=(streamfusion streamfusion_iceberg)
if [[ "${SF_ICEBERG_SOURCE:-iceberg}" == kafka ]]; then
  packages+=(-p streamfusion-kafka -p streamfusion-json)
  libraries+=(streamfusion_kafka streamfusion_json)
fi
cargo build --manifest-path "${REPO_ROOT}/native/Cargo.toml" --release --features mimalloc \
  "${packages[@]}" > "${SF_ICEBERG_ARTIFACTS}/cargo.log" 2>&1
cargo_target="$(cargo metadata --manifest-path "${REPO_ROOT}/native/Cargo.toml" --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
staged="${SF_ICEBERG_ARTIFACTS}/native"
mkdir -p "${staged}"
for library in "${libraries[@]}"; do
  case "$(uname -s)" in Darwin) extension=dylib ;; *) extension=so ;; esac
  cp "${cargo_target}/release/lib${library}.${extension}" "${staged}/"
done
python3 - "${REPO_ROOT}" "${SF_ICEBERG_ARTIFACTS}" <<'PY'
import hashlib, json, pathlib, platform, subprocess, sys
repo, output = map(pathlib.Path, sys.argv[1:])
def command(*args): return subprocess.check_output(args, cwd=repo, text=True).strip()
metadata = {
    'commit': command('git', 'rev-parse', 'HEAD'),
    'dirty': bool(command('git', 'status', '--porcelain')),
    'platform': platform.platform(), 'processor': platform.processor(),
    'java': subprocess.run(['java', '-version'], text=True, capture_output=True).stderr.strip(),
    'rust': command('rustc', '--version'),
    'build': 'cargo build --release --features mimalloc -p streamfusion -p streamfusion-iceberg'
             + (' -p streamfusion-kafka -p streamfusion-json'
                if __import__('os').environ.get('SF_ICEBERG_SOURCE') == 'kafka' else ''),
    'javaHeap': __import__('os').environ.get('SF_ICEBERG_JAVA_HEAP', '4g'),
    'libraries': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (output / 'native').iterdir()},
}
if pathlib.Path('/proc/cpuinfo').exists():
    metadata['cpu'] = next((line.split(':', 1)[1].strip() for line in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), '')
(output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
PY
export SF_ICEBERG_BENCHMARK=true SF_ICEBERG_RELEASE_CONFIRMED=true
mvn -B -ntp -f "${REPO_ROOT}/pom.xml" -Piceberg,iceberg-nexmark,bench \
  -pl streamfusion-iceberg -am -Dtest=IcebergNexmarkBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true -Dnative.build.skip=true \
  "-Dnative.resource.directory=${staged}" "-Dsf.extraJvmArgs=-Djava.library.path='${staged}' -Xmx${SF_ICEBERG_JAVA_HEAP:-4g}" \
  test 2>&1 | tee "${SF_ICEBERG_ARTIFACTS}/run.log"
if [[ -z "${SF_ICEBERG_COMPLETED_CELLS:-}" ]]; then
  python3 "${REPO_ROOT}/bin/report-iceberg.py" "${SF_ICEBERG_ARTIFACTS}"
fi
