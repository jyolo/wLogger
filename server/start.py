from flask import Flask
from multiprocessing import Queue
import os

app = Flask(__name__)
queue = Queue()

@app.route('/')
def index():
    for i in range(10):
        print('123123')
        queue.put('123123')

    return 'pid:%s' % (os.getpid())



def start_server():
    app.debug = True
    app.run()

if __name__ == "__main__":

    start_server()