#!/usr/bin/env sh

set -eu

usage() {
  cat <<'EOF' >&2
usage: build-flink-image.sh --tag <image-ref> (--push | --load) [options]

Builds a job-neutral StreamFusion Flink base image.

  --tag <image-ref>       Image tag to create.
  --push                  Build linux/amd64 and linux/arm64, then push a manifest list.
  --load                  Build one platform and load it into the local Docker daemon.
  --platform <platform>   Platform for --load (default: Docker server platform).
  --flink-image <image>   Flink base image (default: flink:2.2.1-scala_2.12-java17).
  --skip-release-build    Reuse the already-built StreamFusion JARs.
EOF
  exit 64
}

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
flink_image=flink:2.2.1-scala_2.12-java17
image_tag=
mode=
platform=
skip_release_build=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tag)
      [ "$#" -ge 2 ] || usage
      image_tag=$2
      shift 2
      ;;
    --push|--load)
      [ -z "$mode" ] || usage
      mode=$1
      shift
      ;;
    --platform)
      [ "$#" -ge 2 ] || usage
      platform=$2
      shift 2
      ;;
    --flink-image)
      [ "$#" -ge 2 ] || usage
      flink_image=$2
      shift 2
      ;;
    --skip-release-build)
      skip_release_build=true
      shift
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$image_tag" ] && [ -n "$mode" ] || usage

command -v docker >/dev/null 2>&1 || {
  echo "Docker with buildx is required." >&2
  exit 69
}
docker buildx version >/dev/null

if [ "$skip_release_build" = false ]; then
  "$repo_root/bin/build-release.sh" --linux-only
fi

artifact_version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
loader_jar=$repo_root/streamfusion-loader/target/streamfusion-loader-$artifact_version.jar
[ -f "$loader_jar" ] || {
  echo "StreamFusion release JARs are missing; run bin/build-release.sh first." >&2
  exit 66
}

if [ "$mode" = "--push" ]; then
  [ -z "$platform" ] || {
    echo "--platform is only valid with --load; --push always publishes both Linux architectures." >&2
    exit 64
  }
  platforms=linux/amd64,linux/arm64
  output=--push
else
  if [ -z "$platform" ]; then
    platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
  fi
  case "$platform" in
    linux/amd64|linux/arm64) ;;
    *)
      echo "--load supports linux/amd64 or linux/arm64, got: $platform" >&2
      exit 64
      ;;
  esac
  platforms=$platform
  output=--load
fi

docker buildx build \
  --platform "$platforms" \
  --build-arg "FLINK_IMAGE=$flink_image" \
  --build-arg "STREAMFUSION_VERSION=$artifact_version" \
  --tag "$image_tag" \
  --file "$repo_root/docker/flink-base.Dockerfile" \
  "$output" \
  "$repo_root"
