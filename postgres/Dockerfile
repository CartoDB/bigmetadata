FROM ubuntu:16.04

RUN apt-get update --fix-missing
RUN apt-get install -yq make wget build-essential postgresql-server-dev-9.5 \
                        postgresql-9.5-plproxy postgresql-9.5-postgis-2.2 \
                        postgresql-plpython-9.5 python2.7-dev rsyslog
COPY rsyslog/10-postgresql.conf /etc/rsyslog.d/
RUN service rsyslog restart

EXPOSE 5432

COPY postgresql.conf /etc/postgresql/9.5/main/

ADD setup.sh /setup.sh
RUN chmod 0755 /setup.sh
RUN /setup.sh

ADD start-postgis.sh /start-postgis.sh
RUN chmod 0755 /start-postgis.sh

CMD /start-postgis.sh
