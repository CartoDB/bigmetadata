#!/bin/bash
countries=(au br ca es eu fr mx uk us)
folders_regexp="^tmp\/\(au\.\|br\.\|ca\.\|es\.\|eu\.\|fr\.\|mx\.\|uk\.\|us\.\).*"
schema_regexp="^(au\.|br\.|ca\.|es\.|eu\.|fr\.|mx\.|uk\.|us\.|tiger\d{4}|acs\d{4}_\dyr|whosonfirst)"

set -e
set -x

cleanup()
{
    # Remove schemas and directories for every country in the defined array
    find tmp/ -maxdepth 1 -regex $folders_regexp -type d | xargs rm -rf
    docker-compose run -d --rm bigmetadata psql -c "do \$\$ declare schema_rec record; begin for schema_rec in (select schema_name from information_schema.schemata where schema_name ~* '$schema_regexp') loop execute 'drop schema \"'|| schema_rec.schema_name ||'\" cascade'; end loop; end; \$\$"
    # Drop observatory schema
    docker-compose run -d --rm bigmetadata psql -c "drop schema observatory cascade"

}

run_build_tasks()
{
    # for country in "${countries[@]}"; do
    make docker-es-all
    sleep 1
    # done
}

rebuild()
{
    cleanup
    run_build_tasks
    /home/ethervoid/.virtualenvs/bigmetadata/bin/python3 scripts/watch_containers.py
}

read -p "You are going to delete EVERYTHING and rebuild all the data. Continue (y/n)?" choice
case "$choice" in
y|Y ) rebuild;;
n|N ) exit 0;;
* ) echo "Invalid option";;
esac
