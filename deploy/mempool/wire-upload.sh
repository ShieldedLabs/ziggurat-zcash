#!/bin/sh
# Copies the closed wire log hours to Vultr Object Storage, then deletes local hours
# older than 14 days. Cron runs it hourly (see README).
set -eu
cd "$(dirname "$0")"
. ./.env
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

# sync skips files already there with the same size. A restart can append to a closed hour,
# and sync then sends it again.
docker run --rm -v "$PWD/data/wire:/wire:ro" -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  amazon/aws-cli s3 sync /wire "s3://$WIRE_S3_BUCKET/wire/" \
  --endpoint-url "$WIRE_S3_ENDPOINT" --exclude "*" --include "wire-*.ndjson.zst"

# set -e stops before this line when the sync fails.
find data/wire -name "wire-*.ndjson.zst" -mtime +14 -delete
