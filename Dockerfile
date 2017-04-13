FROM recessionporn/debian-gdal2

COPY ./requirements.txt /bigmetadata/requirements.txt

RUN apt-get update
RUN apt-get -yq install wget curl unzip libcurl4-gnutls-dev \
                        git cron p7zip-full && apt-get clean

RUN apt-get -yq remove python-pip && apt-get clean
RUN easy_install pip
RUN pip install --upgrade -r /bigmetadata/requirements.txt

EXPOSE 8082

WORKDIR /bigmetadata
CMD ["true"]
