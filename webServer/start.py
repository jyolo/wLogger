from flask import Flask
from flask_pymongo import PyMongo
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from webServer.admin.user import user
from webServer.admin.home import home


app = Flask(__name__)


# start this server from the main.py
def start_web(conf_dict = {}):
    """
    when run in subprocess need conf_dict of the flask config
    :param conf_dict:
    :return:
    """
    if(not conf_dict ):
        raise ValueError('miss flask config of args conf_dict')

    app.debug = conf_dict['debug']
    app.env = conf_dict['env']
    app.secret_key = conf_dict['secret_key']

    app.config.from_mapping(conf_dict)
    app.register_blueprint(home, url_prefix='/')
    app.register_blueprint(user, url_prefix='/user')


    setAppDataEngine(conf_dict)

    app.run()

def setAppDataEngine(conf_dict):

    if conf_dict['data_engine'] == 'mongodb':
        args = conf_dict[conf_dict['data_engine']]
        if args['username'] and args['password']:
            mongourl = 'mongodb://%s:%s@%s:%s/%s' % (
            args['username'], args['password'], args['host'], args['port'], args['db'])
        else:
            mongourl = 'mongodb://%s:%s/%s' % (args['host'], args['port'], args['db'])

        app.mongo = PyMongo(app,mongourl)

    if conf_dict['data_engine'] == 'mysql':
        pass


def CustomerResponse(data):
    return '123'


if __name__ == "__main__":

    pass

    # app.run()
