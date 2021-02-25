FROM ubuntu:20.04

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y build-essential libssl-dev libffi-dev python3 python3-dev
RUN apt-get install -y python3-pip
RUN apt-get install -y libsnappy-dev

# set the working directory in the container
WORKDIR /code
RUN export PYTHONPATH="$PYTHONPATH:/code"

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
ENTRYPOINT ["sh", "./run.sh"]