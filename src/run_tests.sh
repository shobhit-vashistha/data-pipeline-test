#!/bin/bash

source env.sh
python3 -u test/main.py -test

exec "$@"
