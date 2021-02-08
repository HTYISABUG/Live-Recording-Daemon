import datetime
import logging
import os
import requests
import shutil
import threading

from http import HTTPStatus

from streamlink import Streamlink

session = Streamlink()
logger = logging.getLogger('app')


def download(folder: str, data: dict):
    status_code, url = follow_redirect(data['url'])

    if status_code != HTTPStatus.OK:
        raise Exception(
            f'Status error while following redirect from {data["url"]}')

    data['url'] = url

    filename = None

    if data['platform'] == 'YouTube':
        date_time = datetime.datetime.now().strftime('%Y%m%d.%H%M%S')
        filename = f"{data['platform']}.{data['channelID']}.{data['videoID']}.{date_time}.ts"
    else:
        raise Exception('Invalid platform')

    def f():
        streams = session.streams(data['url'])
        stream_fd = streams['best'].open()

        with open(os.path.join(folder, filename), 'wb') as fd:
            shutil.copyfileobj(stream_fd, fd)

        logger.info(
            f'Stream ended. File write to {os.path.join(folder, filename)}')

    threading.Thread(target=f).start()

    return HTTPStatus.OK, None


def follow_redirect(url: str):
    resp = requests.get(url)
    return resp.status_code, resp.url
