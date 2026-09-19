#!/usr/bin/env sh

set -eu

case "${1:-}" in
  jobmanager|taskmanager|standalone-job|history-server)
    flink_lib="${FLINK_HOME:-/opt/flink}/lib"
    "${JAVA_HOME}/bin/java" \
      -cp "$flink_lib/00-streamfusion-loader.jar:$flink_lib/*" \
      org.apache.flink.table.planner.loader.PlannerModule
    ;;
esac

exec /docker-entrypoint.sh "$@"
