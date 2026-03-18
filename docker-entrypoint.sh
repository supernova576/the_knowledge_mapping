#!/bin/sh
set -e

# Change Github Key Perms to 0400 (Workaround for Windows Volume Sharing)
mkdir -p /run/secrets
cp /tmp/github_key /run/secrets/github_key
chmod 400 /run/secrets/github_key

# rm old app-logs
rm /app/logs/app.log | echo -e "Could not remove logs... Continue"

# rm exports
rm -rf /app/output/exports/* | echo -e "Could not delete exports... Continue"

exec "$@"