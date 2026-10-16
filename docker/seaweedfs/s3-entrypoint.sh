#!/bin/sh
# SeaweedFS (master + volume + filer + S3 в одном процессе). Конфиг доступа S3
# собирается из окружения во временный файл, в образ и в лог не попадает.
set -eu
umask 077
printf '{"identities":[{"name":"admin","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List","Tagging"]}]}' \
  "$S3_ACCESS_KEY" "$S3_SECRET_KEY" > /tmp/s3.json
exec weed server -dir=/data -ip.bind=0.0.0.0 -s3 -s3.port=8333 -s3.config=/tmp/s3.json \
  -master.volumeSizeLimitMB=1024 -volume.max=0
