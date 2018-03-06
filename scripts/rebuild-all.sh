#!/bin/bash
tasks=(au br ca es eu fr mx uk)
heavy_tasks=(us)
folders_regexp="^tmp\/\(au\.\|br\.\|ca\.\|es\.\|eu\.\|fr\.\|mx\.\|uk\.\|us\.\).*"
schema_regexp="^(au\.|br\.|ca\.|es\.|eu\.|fr\.|mx\.|uk\.|us\.|tiger\d{4}|acs\d{4}_\dyr|whosonfirst)"
if [ -z ${VIRTUAL_ENV+x} ]; then python_binary=python3; else python_binary=$VIRTUAL_ENV/bin/python3; fi
run_heavy_tasks=${RUN_HEAVY_TASKS:-true}

set -e

cleanup()
{
    # Remove schemas and directories for every country in the defined array
    find tmp/ -maxdepth 1 -regex $folders_regexp -type d | xargs sudo rm -rf
    docker-compose run -d --rm bigmetadata psql -c "do \$\$ declare schema_rec record; begin for schema_rec in (select schema_name from information_schema.schemata where schema_name ~* '$schema_regexp') loop execute 'drop schema \"'|| schema_rec.schema_name ||'\" cascade'; end loop; end; \$\$"
    # Drop observatory schema
    docker-compose run -d --rm bigmetadata psql -c "drop schema observatory cascade"
}

execute_tasks()
{
    start_time=$(date +'%Y-%m-%d %H:%M:%S' --utc)
    for task in "${tasks[@]}"; do
        make docker-$task-all SCHEDULER=--local-scheduler
        sleep 3
    done
}

execute_heavy_tasks()
{
    heavy_task_start_time=$(date +'%Y-%m-%d %H:%M:%S' --utc)
    for task in "${heavy_tasks[@]}"; do
        make docker-$task-all SCHEDULER=--local-scheduler
    done
}

rebuild()
{
    cleanup
    execute_tasks
    $python_binary scripts/watch_containers.py bigmetadata_bigmetadata_run --since "$start_time" --notification-channel slack
    if [ "$run_heavy_tasks" = true ]; then
        execute_heavy_tasks
        $python_binary scripts/watch_containers.py bigmetadata_bigmetadata_run --since "$heavy_task_start_time" --notification-channel slack
    else
        echo "Skipping heavy tasks..."
    fi
}

read -p "You are going to delete EVERYTHING and rebuild all the data. Continue (y/n)?" choice
case "$choice" in
y|Y ) rebuild;;
n|N ) exit 0;;
* ) echo "Invalid option";;
esac
