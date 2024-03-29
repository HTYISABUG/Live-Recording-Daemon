import datetime
import logging
import os
import requests
import threading

from itertools import chain
from functools import partial

import ffmpeg

from streamlink import Streamlink, StreamError

from utils import follow_redirect

logger = logging.getLogger('app')


def record(folder: str, data: dict):
    # Server info
    remote = data['remote']

    # Download info
    url = follow_redirect(data['url'])

    def output_stream():
        session = Streamlink()

        while True:
            streams = session.streams(url)
            stream = streams['best']

            retry_open = 3
            success_open = False

            for i in range(retry_open):
                try:
                    stream_fd, prebuffer = open_stream(stream)
                    success_open = True
                    break
                except StreamError as err:
                    logger.error(
                        "Try {0}/{1}: Could not open stream {2} ({3})".format(i + 1, retry_open, stream, err))

            if not success_open:
                logger.error(
                    f"Could not open stream {stream}, tried {retry_open} times")
                data['success'] = False
                requests.post(
                    f'http://{remote}/recorder', json=data, timeout=5)
                break

            data['filename'] = filename = get_filename(data)

            with open(os.path.join(folder, filename), 'wb') as output:
                logger.debug("Writing stream to output")
                data['success'] = read_stream(stream_fd, output, prebuffer)

            if data['success']:
                logger.info(
                    f'Stream ended. File write to {os.path.join(folder, filename)}')

                # Start video post-processing
                data['filename'] = postprocess(os.path.join(folder, filename))

                # Send finished notice
                resp: requests.Response = \
                    requests.post(
                        f'http://{remote}/recorder', json=data, timeout=5)

                if not resp.json()['retry']:
                    break
            else:
                requests.post(
                    f'http://{remote}/recorder', json=data, timeout=5)
                break

    threading.Thread(target=output_stream).start()


def get_filename(data):
    if data['platform'] == 'YouTube':
        date_time = datetime.datetime.now().strftime('%Y%m%d.%H%M%S')
        filename = f"{data['platform']}.{data['channelID']}.{date_time}.{data['videoID']}.ts"
    else:
        raise Exception('Invalid platform')

    return filename


def open_stream(stream):
    """Opens a stream and reads 8192 bytes from it.

    This is useful to check if a stream actually has data
    before opening the output.

    """
    # Attempts to open the stream
    try:
        stream_fd = stream.open()
    except StreamError as err:
        raise StreamError("Could not open stream: {0}".format(err))

    # Read 8192 bytes before proceeding to check for errors.
    # This is to avoid opening the output unnecessarily.
    try:
        logger.debug("Pre-buffering 8192 bytes")
        prebuffer = stream_fd.read(8192)
    except OSError as err:
        stream_fd.close()
        raise StreamError("Failed to read data from stream: {0}".format(err))

    if not prebuffer:
        stream_fd.close()
        raise StreamError("No data returned from stream")

    return stream_fd, prebuffer


def read_stream(stream, output, prebuffer, chunk_size=8192) -> bool:
    """Reads data from stream and then writes it to the output."""
    done = True
    stream_iterator = chain(
        [prebuffer],
        iter(partial(stream.read, chunk_size), b"")
    )

    try:
        for data in stream_iterator:
            try:
                output.write(data)
            except OSError as err:
                logger.error(f"Error when writing to output: {err}")
                done = False
                break
    except OSError as err:
        logger.info(f"Error when reading from stream: {err}")
    finally:
        stream.close()

    return done


def postprocess(filepath: str) -> str:
    root, _ = os.path.splitext(filepath)

    p = (
        ffmpeg
        .input(filepath)
        .output(root+'.mp4', vcodec='copy')
        .global_args('-hide_banner', '-y')
        .overwrite_output()
        .run_async(pipe_stdout=True, pipe_stderr=True)
    )
    _, err = p.communicate()

    if p.returncode != 0:
        print(err.decode())

        if os.path.exists(root+'.mp4'):
            os.remove(root+'.mp4')

        return os.path.basename(filepath)

    logger.info(f'Re-encode ended. File write to {root+".mp4"}')

    if os.path.exists(root+'.mp4'):
        logger.info(f'Remove origin file {os.path.basename(filepath)}')

        os.remove(filepath)

    return os.path.basename(root)+'.mp4'
