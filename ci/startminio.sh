#!/bin/sh
set -eux

PORT=9000
PLATFORM=`uname | tr '[:upper:]' '[:lower:]'`

[ -f minio ] || \
    curl -sfSo minio "https://dl.minio.io/server/minio/release/$PLATFORM-amd64/minio"
chmod +x ./minio
./minio --version

export MINIO_ACCESS_KEY=minio MINIO_SECRET_KEY=minio123
./minio server --address localhost:$PORT . &
sleep 2;
