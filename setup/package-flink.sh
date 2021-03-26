#!/bin/bash

proj_dir="${1:-${PWD}}"
branch="${2:-master}"
proj="sunbird-data-pipeline"
repo="https://github.com/project-sunbird/sunbird-data-pipeline.git"

cd proj_dir || echo -e "ERROR: ${proj_dir} does not exist\n" && exit 1
git clone "${repo}"
cd proj || echo -e "ERROR: ${proj} does not exist, git clone has failed\n" && exit 1
git checkout "${branch}"

flink_dir="data-pipeline-flink"

cd proj_dir || echo -e "ERROR: ${proj_dir} does not exist\n" && exit 1
cd flink_dir || echo -e "ERROR: ${flink_dir} does not exist\n" && exit 1

mvn clean package || echo -e "ERROR: error running command: mvn clean package\n" && exit 1
