FROM kartoza/postgis:9.5-2.2

RUN apt-get update --fix-missing
RUN apt-get install -yq make wget build-essential libxml2-dev libproj-dev
RUN apt-get install -yq libgdal-dev
RUN apt-get install -yq postgresql-server-dev-9.5 \
                        postgresql-9.5-plproxy postgresql-plpython-9.5 python2.7-dev \
                        rsyslog

RUN wget http://download.osgeo.org/geos/geos-3.5.1.tar.bz2 && \
    tar xjpf geos-3.5.1.tar.bz2 && \
    cd geos-3.5.1 && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    rm -rf ../geos-3.5.1*

RUN wget http://download.osgeo.org/postgis/source/postgis-2.2.5.tar.gz && \
    tar xzf postgis-2.2.5.tar.gz && \
    cd postgis-2.2.5 && \
    sed -i "s=echo '\\\echo Use=\\/bin\\/echo '\\\echo Use=" extensions/postgis/Makefile.in && \
    ./configure && \
    make && \
    make install && \
    rm -rf ../postgis-2.2.5*

COPY conf/postgresql.conf /etc/postgresql/9.5/main/
RUN chown postgres:postgres /etc/postgresql/9.5/main/postgresql.conf
COPY conf/rsyslog/10-postgresql.conf /etc/rsyslog.d/

COPY scripts/start.sh /
RUN chmod 0755 /start.sh

CMD /start.sh
