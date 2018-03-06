import docker
import time
import argparse
import os
import requests
import logging
import logging.config
import json
from datetime import datetime
from datetime import timedelta

STDOUT = "stdout"
SLACK = "slack"
NOTIFICATION_CHANNELS = [STDOUT, SLACK]
DOCKER_SOCKET_URL = 'unix://var/run/docker.sock'
DEFAULT_POLLING_TIME = 60
DOCKER_STATUS_RUNNING = 'running'
DEFAULT_LOGGING_CONF = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'conf', 'logging_client.cfg')


def watch_containers(name, since=datetime.utcnow(), polling_time=DEFAULT_POLLING_TIME, notification_channel=STDOUT, logger=None):
    init_time = datetime.utcnow()
    api_client = docker.APIClient(base_url=DOCKER_SOCKET_URL)
    client = docker.from_env()
    containers = get_active_containers(client, api_client, name, since)
    successful_containers = []
    failed_containers = []
    deleted_containers = []
    while(True):
        active_containers = get_active_containers(client, api_client, name, since)
        if not active_containers:
            logger.info("No more active containers. Exiting...")
            break
        else:
            logger.info("Still running containers. Sleeping...")
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
    finish_time = datetime.utcnow() - init_time
    finish_time_in_minutes = finish_time / timedelta(minutes=1)
    message = "Finished build. Total {} -- Successful {} -- Failed {} -- Deleted {}.\nIt took {} minutes".format(len(containers), len(successful_containers), len(failed_containers), len(deleted_containers), int(finish_time_in_minutes))
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
        logger.info(message)
    elif channel == SLACK:
        if 'SLACK_WEBHOOK' not in os.environ:
            logger.error('No SLACK WEBHOOK provided')
            # Fallback to have it at least in the log file
            logger.info(message)
            return
        webhook = os.environ['SLACK_WEBHOOK']
        try:
            payload = json.dumps({'text': message})
            response = requests.post(webhook, data=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.exception('Can\'t send SLACK notification')
    else:
        logger.error('Notification channel option unknown')


if __name__ == "__main__":
    parser = argparse.ArgumentParser('python3 watch_containers.py')
    parser.add_argument('name', help='Name, complete or just a part, of the container to check')
    parser.add_argument('--since', default=datetime.utcnow(), type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), help='Date where the containers where started. Supported format YYYY-mm-dd HH:MM:SS and should be in UTC')
    parser.add_argument('--polling-time', default=60, type=int, help='Time, in seconds, to check for the state of the active containers')
    parser.add_argument('--notification-channel', default=STDOUT, choices=NOTIFICATION_CHANNELS, help='Notification channel')
    parsed_args = vars(parser.parse_args())
    logging.config.fileConfig(DEFAULT_LOGGING_CONF)
    logger = logging.getLogger('watcher')
    parsed_args['logger'] = logger
    watch_containers(**parsed_args)
