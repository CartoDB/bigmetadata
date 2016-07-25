FROM debian:jessie

RUN apt-get update --fix-missing
RUN apt-get -yq install python-pip python-dev libpq-dev postgresql-client-common \
                        postgresql-client-9.4 wget curl unzip build-essential \
                        libcurl4-gnutls-dev libproj-dev texlive-latex-base \
                        texlive-fonts-recommended texlive-fonts-extra \
                        texlive-latex-extra libjpeg-dev git

ENV GDAL_VERSION 2.1.1
ADD http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz /usr/local/src/

RUN cd /usr/local/src && tar -xvf gdal-${GDAL_VERSION}.tar.gz && cd gdal-${GDAL_VERSION} \
    && ./configure --with-python --with-pg --with-curl --with-proj \
    && make && make install && ldconfig \
    && rm -Rf /usr/local/src/*

COPY ./requirements.txt /bigmetadata/requirements.txt
RUN pip install -r /bigmetadata/requirements.txt

EXPOSE 8082

WORKDIR /bigmetadata
CMD luigid
