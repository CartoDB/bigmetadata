import docker
import time
import argparse
from datetime import datetime

def watch_containers(name, since=datetime.now(), pooling_time=60):
    api_client = docker.APIClient(base_url='unix://var/run/docker.sock')
    client = docker.from_env()
    containers = get_active_containers(client, api_client, name, since)
    successful_containers = []
    failed_containers = []
    while(True):
        active_containers = get_active_containers(client, api_client, name, since)
        if not active_containers:
            print("No more active containers. Exiting...")
            break
        else:
            print("Still running containers. Sleeping...")
            time.sleep(pooling_time)

    for container in containers:
        exit_code = api_client.inspect_container(container.id)['State']['ExitCode']
        if exit_code > 0:
            failed_containers.append(container)
        else:
            successful_containers.append(container)
    send_notification("Finished. Total {} -- Successful {} -- Failed {}".format(len(containers), len(successful_containers), len(failed_containers)))

def get_active_containers(client, api_client, name, since):
    active_containers = []
    for container in client.containers.list(filters = {'status': 'running', 'name': name}):
        container_started_at = api_client.inspect_container(container.id)['State']['StartedAt'].split('.')[0]
        starting_date = datetime.strptime(container_started_at, '%Y-%m-%dT%H:%M:%S')
        if starting_date >= since:
            active_containers.append(container)
    return active_containers

def send_notification(message):
    print(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser('python3 watch_containers.py')
    parser.add_argument('name', help='Name, complete or just a part, of the container to check')
    parser.add_argument('--since', type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), help='Date where the containers where started. Supported format YYYY-mm-dd HH:MM:SS')
    parser.add_argument('--pooling-time', default=60, type=int, help='Time, in seconds, to check for the state of the active containers')
    parsed_args = vars(parser.parse_args())
    watch_containers(**parsed_args)
