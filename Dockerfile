FROM ubuntu:xenial

COPY ./requirements.txt /bigmetadata/requirements.txt
RUN apt-get update
RUN apt-get -y install make build-essential wget curl unzip git p7zip-full software-properties-common vim inetutils-ping htop
RUN add-apt-repository -y ppa:cartodb/postgresql-10
RUN add-apt-repository -y ppa:cartodb/nodejs
RUN apt-get update --fix-missing

RUN apt-get -y install nodejs postgresql-client-10 postgresql-server-dev-10 postgresql-server-dev-9.5 gdal-bin python3-pip

# Mapshaper
RUN npm install -g mapshaper

# Luigi
RUN pip3 install --upgrade -r /bigmetadata/requirements.txt

# Luigi Web UI
EXPOSE 8082

COPY ./scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod 0755 /usr/local/bin/wait-for-it.sh
ENV PATH /usr/local/bin:$PATH

WORKDIR /bigmetadata
CMD ["true"]
