#!/bin/bash

source env.sh

cd web
python3 manage.py runserver localhost:6660
