# No explicit tag for 9.5-2.2 yet, but that's what we use
FROM kartoza/postgis:latest

RUN apt-get update --fix-missing
RUN apt-get install -yq make wget build-essential postgresql-server-dev-9.5 \
                             libxml2-dev libgdal-dev libproj-dev libjson0-dev \
                             ruby python-pip postgresql-9.5-plproxy \
                             postgresql-plpython-9.5 python2.7-dev

RUN wget http://download.osgeo.org/geos/geos-3.5.0.tar.bz2 && \
    tar xjpf geos-3.5.0.tar.bz2 && \
    cd geos-3.5.0 && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    rm -rf ../geos-3.5.0*

RUN wget http://download.osgeo.org/postgis/source/postgis-2.2.1.tar.gz && \
    tar xzf postgis-2.2.1.tar.gz && \
    cd postgis-2.2.1 && \
    ./configure && \
    make && \
    make install && \
    rm -rf ../postgis-2.2.1*
