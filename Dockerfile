FROM ubuntu:xenial

COPY ./requirements.txt /bigmetadata/requirements.txt

RUN apt-get update

# PostgreSQL (client + dev headers for psycopg2 compilation)
RUN apt-get -y install postgresql-client libpq-dev

# ogr2ogr
RUN apt-get -y install gdal-bin

# mapshaper
RUN apt-get -y install npm nodejs-legacy
RUN npm install -g mapshaper

# Shell utils used in ETL tasks (TODO: remove them)
RUN apt-get -y install wget curl unzip git p7zip-full

# Luigi
RUN apt-get -y install python3-pip
RUN pip3 install --upgrade -r /bigmetadata/requirements.txt

# Luigi Web UI
EXPOSE 8082

WORKDIR /bigmetadata
CMD ["true"]
