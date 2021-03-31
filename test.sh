#! /usr/bin/env sh
set -eux

CID=$(docker run --detach --env SERVICES=s3 --publish 4566:4566 localstack/localstack)
trap "docker stop ${CID}" EXIT
while ! curl http://localhost:4566/health; do
    sleep 10
done

docker run \
       --env AWS_ACCESS_KEY_ID=test --env AWS_SECRET_ACCESS_KEY=test --network host \
       amazon/aws-cli --endpoint-url=http://localhost:4566 s3 mb s3://rusoto-s3-mpu

cargo fmt -- --check
env AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    ENDPOINT=http://localhost:4566 \
    BUCKET=rusoto-s3-mpu \
    cargo test
