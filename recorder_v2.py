import os
import logging
import threading
import requests

from contextlib import closing
from datetime import datetime
from functools import partial
from itertools import chain

import ffmpeg

from requests.models import Response
from streamlink import StreamError, Streamlink
from streamlink_cli.output import FileOutput

from utils import redirect

retry_open = 3
logger = logging.getLogger('app')


def record(data: dict):
    callback_url = data['callback']
    stream_url = redirect(data['url'])

    def target():
        session = Streamlink()

        while True:
            # Open stream
            streams = session.streams(stream_url)
            stream = streams['best']

            # Start recording
            data['success'], filepath = output_stream(data, stream)

            if data['success']:
                logger.info(f'Stream ended.\tFile write to {filepath}')

                try:
                    # Start post-processing
                    data['filename'] = postprocess(filepath)
                except Exception as e:
                    # Discard post-processing
                    logger.error(e)
                    data['filename'] = os.path.basename(filepath)

                resp = callback(callback_url, data)

                try:
                    if not resp.json()['retry']:
                        break
                except Exception as e:
                    print(e)
                    print(resp.text)
                    break
            else:
                callback(callback_url, data)
                break

    threading.Thread(target=target).start()


def output_stream(data, stream) -> bool:
    """Open stream, create output and finally write the stream to output."""
    success_open = False

    for i in range(retry_open):
        try:
            stream_fd, prebuffer = open_stream(stream)
            success_open = True
            break
        except StreamError as err:
            logger.error("Try {0}/{1}: Could not open stream {2} ({3})".format(
                i + 1, retry_open, stream, err))

    if not success_open:
        logger.error(
            "Could not open stream {0}, tried {1} times", stream, retry_open)
        return False, None

    output = create_output(data)

    try:
        output.open()
    except OSError as err:
        logger.error("Failed to open output: {0} ({1})", output.filename, err)

    with closing(output):
        logger.debug("Writing stream to output")
        done = read_stream(stream_fd, output, prebuffer)

    return done, output.filename


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


def create_output(data: dict) -> FileOutput:
    """Decides where to write the stream.

    Depending on arguments it can be one of these:
     - A regular file

    """
    filename = get_filename(data)
    filepath = os.path.join(data['savepath'], filename)
    return FileOutput(filepath)


def get_filename(data: dict) -> str:
    if data['platform'] == 'YouTube':
        date_time = datetime.now().strftime('%Y%m%d.%H%M%S')
        filename = f"{data['platform']}.{data['channelID']}.{date_time}.{data['videoID']}.ts"
    else:
        raise NotImplementedError('Not implemented platform')

    return filename


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
                logger.error("Error when writing to output: {0}, exiting", err)
                done = False
                break
    except OSError as err:
        logger.debug("Error when reading from stream: {0}, exiting", err)
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
        if os.path.exists(root + '.mp4'):
            os.remove(root + '.mp4')

        raise Exception(err.decode())

    logger.info(f'Re-encode ended.\tFile write to {root+".mp4"}')

    if os.path.exists(root + '.mp4'):
        logger.debug(f'Remove origin file {os.path.basename(filepath)}')
        os.remove(filepath)

    return os.path.basename(root)+'.mp4'


def callback(url: str, data: dict) -> Response:
    """Send callback request to notification server."""
    return requests.post(f'http://{url}', json=data, timeout=5)
