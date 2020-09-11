from flask import Flask
from environs import Env
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.admin.user import user
from server.admin.home import home



app = Flask(__name__)

# start this server from the main.py
def start_server(conf_dict = {}):
    """
    when run in subprocess need conf_dict of the flask config
    :param conf_dict:
    :return:
    """
    if(not conf_dict and __name__ != '__main__'):
        raise ValueError('miss flask config of args conf_dict')
    else:
        app.config.from_pyfile( os.path.dirname(os.path.abspath(__file__)) + '/config.py' )

    app.config.from_mapping(conf_dict)
    app.register_blueprint(home, url_prefix='/')
    app.register_blueprint(user, url_prefix='/user')


    app.run()



if __name__ == "__main__":

    start_server()

    # app.run()
