FROM debian:latest

RUN apt-get update
RUN apt-get -yq install python-pip python-dev libpq-dev postgresql-client-common \
                        postgresql-client-9.4 wget curl gdal-bin

COPY ./requirements.txt /bigmetadata/requirements.txt

RUN apt-get -yq remove python-pip
RUN easy_install pip
RUN pip install -r /bigmetadata/requirements.txt

EXPOSE 8082

WORKDIR /bigmetadata
CMD luigid
