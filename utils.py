import requests


def follow_redirect(url: str):
    return requests.get(url).url
