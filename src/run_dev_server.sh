#!/bin/bash

source env.sh

cd web
python manage.py runserver localhost:6660
