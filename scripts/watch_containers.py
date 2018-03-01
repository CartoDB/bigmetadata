import docker
import time
import argparse
from datetime import datetime
from datetime import timedelta


def watch_containers(name, since=datetime.now(), pooling_time=60, notification_channel='stdout'):
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
    finish_time = datetime.utcnow() - since
    finish_time_in_minutes = finish_time / timedelta(minutes=1)
    message = "Finished. Total {} -- Successful {} -- Failed {}. It tooks {} minutes".format(len(containers), len(successful_containers), len(failed_containers), finish_time_in_minutes)
    send_notification(notification_channel, message)


def get_active_containers(client, api_client, name, since):
    active_containers = []
    for container in client.containers.list(filters={'status': 'running', 'name': name}):
        container_started_at = api_client.inspect_container(container.id)['State']['StartedAt'].split('.')[0]
        starting_date = datetime.strptime(container_started_at, '%Y-%m-%dT%H:%M:%S')
        if starting_date >= since:
            active_containers.append(container)
    return active_containers


def send_notification(channel, message):
    if channel == 'stdout':
        print(message)
    elif channel == 'slack':
        raise NotImplementedError('Slack notification is not implemented yet')
    elif channel == 'logfile':
        raise NotImplementedError('Log file notification is not implemented yet')
    else:
        raise ('Notification channel option unknown')


if __name__ == "__main__":
    parser = argparse.ArgumentParser('python3 watch_containers.py')
    parser.add_argument('name', help='Name, complete or just a part, of the container to check')
    parser.add_argument('--since', type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), help='Date where the containers where started. Supported format YYYY-mm-dd HH:MM:SS')
    parser.add_argument('--pooling-time', default=60, type=int, help='Time, in seconds, to check for the state of the active containers')
    parser.add_argument('--notification-channel', default='stdout', choices=['stdout', 'slack', 'logfile'], help='Notification channel')
    parsed_args = vars(parser.parse_args())
    watch_containers(**parsed_args)
