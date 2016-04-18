FROM dataquestio/python2-starter

USER root
RUN apt-get update
RUN apt-get install -yq libpq-dev postgresql-client-common postgresql-client-9.3

USER ds
COPY requirements.txt /bigmetadata/ipython/requirements.txt
RUN /opt/ds/bin/pip install -r /bigmetadata/ipython/requirements.txt
