import logging
import json
import os

from http import HTTPStatus

from flask import Flask, Response, request, abort
from flask.logging import create_logger
from flask_httpauth import HTTPTokenAuth

from downloader import download

app = Flask(__name__)
auth = HTTPTokenAuth()

logger = logging.getLogger('werkzeug')
logger.setLevel(logging.WARNING)

logger = create_logger(app)
logger.setLevel(logging.INFO)

with open('settings.json') as fp:
    settings = json.load(fp)
    token = settings['token']
    savepath = settings['savepath']

    if not os.path.exists(savepath):
        os.makedirs(savepath)


@app.route('/', methods=['POST'])
@auth.login_required
def main():
    data = request.json

    try:
        download(savepath, data)
    except Exception as e:
        abort(HTTPStatus.INTERNAL_SERVER_ERROR, err_msg(e))

    logger.info(f'Live on {data["url"]} start recording')

    return Response(status=HTTPStatus.OK)


@auth.verify_token
def verify_token(t):
    if t == token:
        return ''


def err_msg(e: Exception):
    """500 bad request for exception

    Returns:
        500 and msg which caused problems
    """
    error_class = e.__class__.__name__
    detail = e.args[0]

    _, _, tb = sys.exc_info()
    lastCallStack = traceback.extract_tb(tb)[-1]
    file_name = lastCallStack[0]  # Exception file name
    line_num = lastCallStack[1]  # Exception line number
    func_name = lastCallStack[2]  # Exception function name

    # generate the error message
    msg = f'Exception raise in file: {file_name}, line {line_num}, in {func_name}: [{error_class}] {detail}.'

    return msg


if __name__ == '__main__':
    app.run(port=5148)
