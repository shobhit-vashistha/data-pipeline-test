FROM ubuntu:20.04

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y build-essential libssl-dev libffi-dev python3 python3-dev
RUN apt-get install -y python3-pip
RUN apt-get install -y libsnappy-dev

VOLUME /mock-kafka-data

# set the working directory in the container
WORKDIR /src
RUN export PYTHONPATH="$PYTHONPATH:/src"

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# copy the content of the local src directory to the working directory
COPY . .

RUN chmod u+x ./run_server.sh

# command to run on container start
ENTRYPOINT ["/bin/bash", "-c", "./run_server.sh"]