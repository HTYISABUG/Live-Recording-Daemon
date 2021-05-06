import logging
import requests
import threading
import os

import youtube_dl

from utils import follow_redirect

# Download status
STATUS_DOWNLOADING = "downloading"
STATUS_ERROR = "error"
STATUS_FINISHED = "finished"

logger = logging.getLogger('app')
options = {
    'outtmpl': '%(title)s.%(id)s.%(ext)s',
    'logger': logger,
}


def download(folder: str, data: dict):
    # Server info
    remote = data['remote']

    # Download info
    urls = [follow_redirect(url) for url in data['url']]

    merge = False

    def hook(d: dict):
        nonlocal merge

        if d['status'] == STATUS_FINISHED:
            new_data = data.copy()
            new_data['success'] = True

            second_last = d['filename'].rsplit('.', 2)[-2]

            if len(second_last) != 11:
                new_data['videoID'] = d['filename'].rsplit('.', 3)[-3]
                new_data['filename'] = f"{d['filename'].rsplit('.', 3)[0]}.{new_data['videoID']}.mkv"

                merge = (not merge)
            else:
                new_data['videoID'] = second_last
                new_data['filename'] = d['filename']

            new_data['filename'] = os.path.basename(new_data['filename'])

            if not merge:
                requests.post(
                    f'https://{remote}/recorder', json=new_data, timeout=5)

    def output_file():
        opts = options.copy()
        opts['progress_hooks'] = [hook]
        opts['outtmpl'] = os.path.join(folder, opts['outtmpl'])

        nonlocal merge

        with youtube_dl.YoutubeDL(opts) as dl:
            for u in urls:
                try:
                    merge = False
                    dl.download([u])
                except youtube_dl.DownloadError as e:
                    new_data = data.copy()
                    new_data['success'] = False
                    new_data['description'] = str(e).split(maxsplit=1)[1]

                    requests.post(
                        f'https://{remote}/recorder', json=new_data, timeout=5)
                except Exception as e:
                    new_data = data.copy()
                    new_data['success'] = False
                    new_data['description'] = str(e)

                    requests.post(
                        f'https://{remote}/recorder', json=new_data, timeout=5)

    threading.Thread(target=output_file).start()
