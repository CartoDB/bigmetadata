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

success_finished_containers = client.containers.list(filters = {'status': 'exited', 'exited': '0', 'name': 'bigmetadata_bigmetadata_run'})
finished_containers = client.containers.list(filters = {'status': 'exited', 'name': 'bigmetadata_bigmetadata_run'})
failed_containers = [container for container in finished_containers if container not in set(success_finished_containers)]
print("Finished. Total {} -- Successful {} -- Failed {}".format(len(finished_containers), len(success_finished_containers), len(failed_containers)))
