#!/bin/sh
set -e

mkdir -p /run/secrets
cp /tmp/github_key /run/secrets/github_key
chmod 400 /run/secrets/github_key

exec "$@"