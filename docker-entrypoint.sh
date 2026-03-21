#!/bin/sh
set -e

# rm old app-logs
rm /app/logs/app.log | echo -e "Could not remove logs... Continue"

# rm exports
rm -rf /app/output/exports/* | echo -e "Could not delete exports... Continue"

exec "$@"