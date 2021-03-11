#!/bin/bash

source env.sh

cd web
python3 manage.py runserver 0.0.0.0:6660
