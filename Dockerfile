FROM python:3.8-alpine

# set the working directory in the container
WORKDIR /code
RUN export PYTHONPATH="$PYTHONPATH:/code"

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
ENTRYPOINT ["sh", "./run.sh"]