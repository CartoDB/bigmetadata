FROM debian:jessie

RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get -y update

RUN apt-get -yq install build-essential python-dev python-pip libpq-dev \
                        postgresql-client-common postgresql-client-9.5 \
                        libproj-dev && apt-get clean

ENV GDAL_VERSION 2.1.3
ADD http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz /usr/local/src/

RUN cd /usr/local/src && tar -xvf gdal-${GDAL_VERSION}.tar.gz && cd gdal-${GDAL_VERSION} \
    && ./configure --with-python --with-pg --with-curl --with-proj \
    && make && make install && ldconfig \
    && rm -Rf /usr/local/src/*
