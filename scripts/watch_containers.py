#!/usr/bin/python
import docker
import time
client = docker.from_env()
while(True):
    active_containers = client.containers.list(filters = {'status': 'running', 'name': 'bigmetadata_bigmetadata_run'})
    if not active_containers:
        print("No more active containers. Exiting...")
        break
    else:
        print("Still running containers. Sleeping...")
        time.sleep(60)
