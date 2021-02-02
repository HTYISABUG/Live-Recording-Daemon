from flask import Flask, Response


app = Flask(__name__)


@app.route('/')
def root():
    return Response(status=200)
