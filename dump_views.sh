#!/bin/bash

printf -v now '%(%Y-%m-%d %H:%M:%S)T\n' -1
echo "Started at $now"

mkdir -p database/csv

docker exec -t hapi-db pg_dump -U postgres -Fc hapi -f hapi_db.pg_restore
docker cp hapi-db:/hapi_db.pg_restore database/hapi_db.pg_restore

printf -v now '%(%Y-%m-%d %H:%M:%S)T\n' -1
echo "Ended at $now"
