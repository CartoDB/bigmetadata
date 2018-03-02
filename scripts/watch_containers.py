import docker
import time
import argparse
from datetime import datetime
from datetime import timedelta

STDOUT = "stdout"
SLACK = "slack"
LOGFILE = "logfile"
NOTIFICATION_CHANNELS = [STDOUT, SLACK, LOGFILE]
DOCKER_SOCKET_URL = 'unix://var/run/docker.sock'
DEFAULT_POLLING_TIME = 60
DOCKER_STATUS_RUNNING = 'running'


def watch_containers(name, since=datetime.utcnow(), polling_time=DEFAULT_POLLING_TIME, notification_channel=STDOUT):
    api_client = docker.APIClient(base_url=DOCKER_SOCKET_URL)
    client = docker.from_env()
    containers = get_active_containers(client, api_client, name, since)
    successful_containers = []
    failed_containers = []
    deleted_containers = []
    while(True):
        active_containers = get_active_containers(client, api_client, name, since)
        if not active_containers:
            print("No more active containers. Exiting...")
            break
        else:
            print("Still running containers. Sleeping...")
            time.sleep(polling_time)

    for container in containers:
        try:
            exit_code = api_client.inspect_container(container.id)['State']['ExitCode']
            if exit_code > 0:
                failed_containers.append(container)
            else:
                successful_containers.append(container)
        except docker.errors.APIError:
            deleted_containers.append(container)
    finish_time = datetime.utcnow() - since
    finish_time_in_minutes = finish_time / timedelta(minutes=1)
    message = "Finished. Total {} -- Successful {} -- Failed {} -- Deleted {}. It tooks {} minutes".format(len(containers), len(successful_containers), len(failed_containers), len(deleted_containers), int(finish_time_in_minutes))
    send_notification(notification_channel, message)


def get_active_containers(client, api_client, name, since):
    active_containers = []
    for container in client.containers.list(filters={'status': DOCKER_STATUS_RUNNING, 'name': name}):
        container_started_at = api_client.inspect_container(container.id)['State']['StartedAt'].split('.')[0]
        starting_date = datetime.strptime(container_started_at, '%Y-%m-%dT%H:%M:%S')
        if starting_date >= since:
            active_containers.append(container)
    return active_containers


def send_notification(channel, message):
    if channel == STDOUT:
        print(message)
    elif channel == SLACK:
        raise NotImplementedError('Slack notification is not implemented yet')
    elif channel == LOGFILE:
        raise NotImplementedError('Log file notification is not implemented yet')
    else:
        raise ('Notification channel option unknown')


if __name__ == "__main__":
    parser = argparse.ArgumentParser('python3 watch_containers.py')
    parser.add_argument('name', help='Name, complete or just a part, of the container to check')
    parser.add_argument('--since', default=datetime.utcnow(), type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), help='Date where the containers where started. Supported format YYYY-mm-dd HH:MM:SS')
    parser.add_argument('--polling-time', default=60, type=int, help='Time, in seconds, to check for the state of the active containers')
    parser.add_argument('--notification-channel', default=STDOUT, choices=NOTIFICATION_CHANNELS, help='Notification channel')
    parsed_args = vars(parser.parse_args())
    watch_containers(**parsed_args)
