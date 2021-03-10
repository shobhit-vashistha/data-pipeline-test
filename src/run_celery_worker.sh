#!/bin/sh

sleep 10

celery --app=web worker --concurrency=10 -l INFO --detach

exec "$@"