#!/bin/bash

set -e

LIST_NUM_CLUSTER=( \
  50 100 200 400 800 \
  5 5 5 5 \
  )
LIST_NUM_UNITS=( \
  1000 1000 1000 1000 1000 \
  20000 40000 80000 160000 \
)

if [ "$(docker ps -a -q -f name=postgres-main)" ]; then
    echo "Removing existing postgres-main container..."
    docker rm -f postgres-main
fi

echo "Starting postgres-main container..."
docker run --name postgres-main -p 5433:5432 -e POSTGRES_PASSWORD=postgres --tmpfs /var/lib/postgresql/data -d postgres
sleep 1
docker exec -it postgres-main dropdb -Upostgres fractal-test --if-exists

for idx in "${!LIST_NUM_CLUSTER[@]}"; do
    NUM_CLUSTER="${LIST_NUM_CLUSTER[$idx]}"
    NUM_UNITS="${LIST_NUM_UNITS[$idx]}"

    echo "START CASE $NUM_CLUSTER, $NUM_UNITS"
    echo "Creating fractal_test database..."
    docker exec -it postgres-main createdb -Upostgres fractal-test

    OUT_CREATEDB="out.createdb.${NUM_CLUSTER}.${NUM_UNITS}.txt"
    OUT_BENCH="out.bench_perf.${NUM_CLUSTER}.${NUM_UNITS}.txt"

    poetry run fractalctl set-db > /dev/null 2>&1
    poetry run python create_dbs.py "$NUM_CLUSTER" "$NUM_UNITS" > "$OUT_CREATEDB" 2>&1
    poetry run python bench_perf.py "$NUM_CLUSTER" "$NUM_UNITS" > "$OUT_BENCH" 2>&1

    echo "END CASE $NUM_CLUSTER, $NUM_UNITS"
    echo "----------------------------------------------"
    docker exec -it postgres-main dropdb -Upostgres fractal-test --if-exists
done
