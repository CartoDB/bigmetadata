FROM elasticsearch:latest

RUN apt-get update
RUN apt-get -yq install python-dev python-pip libpq-dev

COPY ./requirements.txt /bigmetadata/requirements.txt
RUN pip install -r /bigmetadata/requirements.txt
