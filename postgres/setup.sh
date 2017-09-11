DATADIRECTORY="/var/lib/postgresql/9.5/main"
CONF="/etc/postgresql/9.5/main/postgresql.conf"
POSTGRES="/usr/lib/postgresql/9.5/bin/postgres"
INITDB="/usr/lib/postgresql/9.5/bin/initdb"
CREATEUSERJ="/usr/lib/postgresql/9.5/bin/initdb"
SQLDIR="/usr/share/postgresql/9.5/contrib/postgis-2.2/"

chmod 600 /etc/ssl/private/ssl-cert-snakeoil.key

# Restrict subnet to docker private network
echo "host    all             all             172.17.0.0/16               md5" >> /etc/postgresql/9.5/main/pg_hba.conf
# And allow access from DockerToolbox / Boottodocker on OSX
echo "host    all             all             192.168.0.0/16               md5" >> /etc/postgresql/9.5/main/pg_hba.conf

if [ ! -d $DATADIRECTORY ]; then
  mkdir -p $DATADIRECTORY
  chown -R postgres:postgres $DATADIRECTORY
  echo "Creating Postgres database at $DATADIRECTORY"
  su - postgres -c "$INITDB $DATADIRECTORY"
  su - postgres -c "$POSTGRES --single -D $DATADIRECTORY -c config_file=$CONF <<< \"CREATE USER $POSTGRES_USER WITH SUPERUSER ENCRYPTED PASSWORD '$POSTGRES_PASS';\""
fi
