FROM recessionporn/debian-gdal2

COPY ./requirements.txt /bigmetadata/requirements.txt

RUN apt-get update
RUN apt-get -yq install wget curl unzip libcurl4-gnutls-dev texlive-latex-base \
                        texlive-fonts-recommended libjpeg-dev git \
                        libfreetype6-dev cron p7zip-full

RUN apt-get -yq remove python-pip
RUN easy_install pip
RUN pip install --upgrade -r /bigmetadata/requirements.txt

EXPOSE 8082

WORKDIR /bigmetadata
CMD ["true"]
