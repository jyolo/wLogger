from flask import Flask
from multiprocessing import Queue
from server.admin.user import user
from server.admin.home import home
from environs import Env
import os


env = Env()
env.read_env()





app = Flask(__name__)

# app.register_blueprint(home,url_prefix='/')
# app.register_blueprint(user,url_prefix='/user')


# def start_server():
#     app.run(debug=True)



if __name__ == "__main__":
    print('123')
    # evn = Env.read_env()
    # print(evn)
    # start_server()