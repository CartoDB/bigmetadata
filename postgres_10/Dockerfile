FROM ubuntu:16.04

ENV PATH /usr/lib/postgresql/10/bin:$PATH
RUN apt-get update && apt-get install -y locales
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN groupadd -r postgres --gid=999 && useradd -r -g postgres --uid=999 postgres

RUN mkdir /docker-entrypoint-initdb.d

RUN apt-get update --fix-missing
RUN apt-get install -yq make wget build-essential vim software-properties-common

RUN add-apt-repository -y ppa:cartodb/postgresql-10
RUN add-apt-repository -y ppa:cartodb/gis
RUN apt-get update --fix-missing

RUN apt-get install -yq libgdal20 libgeos-3.5.1 postgresql-10 postgresql-common postgis rsyslog postgresql-server-dev-10 postgresql-plpython-10 postgresql-plpython3-10

RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 2777 /var/run/postgresql
RUN mkdir -p /var/run/postgresql/10-pg_stat_mem_tmp

COPY conf/postgresql.conf /etc/postgresql/10/main/
RUN chown postgres:postgres /etc/postgresql/10/main/postgresql.conf
COPY conf/pg_hba.conf /etc/postgresql/10/main/
RUN chown postgres:postgres /etc/postgresql/10/main/pg_hba.conf
RUN chmod 0400 /etc/postgresql/10/main/pg_hba.conf
COPY conf/rsyslog/10-postgresql.conf /etc/rsyslog.d/

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
RUN ln -s /usr/local/bin/docker-entrypoint.sh /docker-entrypoint.sh

EXPOSE 5432
ENTRYPOINT ["docker-entrypoint.sh"]
