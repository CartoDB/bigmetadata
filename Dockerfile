FROM elasticsearch:latest

RUN apt-get update
RUN apt-get -yq install python-dev python-pip libpq-dev postgresql-client-common \
                        postgresql-client-9.4

COPY ./requirements.txt /bigmetadata/requirements.txt
RUN pip install -r /bigmetadata/requirements.txt
