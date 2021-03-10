#!/bin/bash

source env.sh
gunicorn -w 2 --chdir /src/web web.wsgi:application --bind 0.0.0.0:6660

exec "$@"
