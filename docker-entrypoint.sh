#!/bin/sh
set -e

# rm old app-logs
rm /app/logs/app.log || echo -e "Could not remove logs... Continue"

# rm exports
rm -rf /app/output/exports/* || echo -e "Could not delete exports... Continue"

# rm output/ai_feedback_error
rm -rf /app/output/ai_feedback_error/* || echo -e "Could not delete error dumps... Continue"

exec "$@"