#!/usr/bin/env bash
# Runs tests
set -euxo pipefail

if [[ -z ${KOCHIKU_ENV+x} ]]; then
  echo "running in local mode"
  GIT_COMMIT=$(git rev-parse HEAD)
fi

BUILD_DOCKER_TAG=square-vitess-build-${GIT_COMMIT}

if [[ -n ${KOCHIKU_ENV+x} ]]; then
  # if we are running inside of kochiku, first load the docker container
  docker load -i docker-cache/"${BUILD_DOCKER_TAG}".tar
fi

docker run "$BUILD_DOCKER_TAG" go run test.go -docker=false -timeout=8m -exclude=square-fail -print-log -shard "$1"
