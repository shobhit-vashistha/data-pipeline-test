#!/bin/bash

proj_dir="${1:-${PWD}}"
branch="${2:-master}"
proj="sunbird-data-pipeline"
repo="https://github.com/project-sunbird/sunbird-data-pipeline.git"

cd proj_dir || echo "ERROR: ${proj_dir} does not exist" && exit 1
git clone "${repo}"
cd proj || echo "ERROR: ${proj} does not exist, git clone has failed" && exit 1
git checkout "${branch}"

