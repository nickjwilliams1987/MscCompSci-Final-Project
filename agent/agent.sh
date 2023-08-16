#!/bin/bash

cd "$(dirname "$0")"
echo "In dir: " `pwd`
poetry update

while true; do
  prefect agent start -q main
  echo "In dir: " `pwd`

  echo "Prefect Agent crashed! with exit $?. Restarting.." >&2
  sleep 1
done
