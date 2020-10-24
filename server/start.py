from flask import Flask
from flask_pymongo import PyMongo,MongoClient
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.admin.user import user
from server.admin.home import home




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
    app.config.from_mapping(conf_dict)
    app.register_blueprint(home, url_prefix='/')
    app.register_blueprint(user, url_prefix='/user')

    setAppDataEngine(app,conf_dict)

    app.run()

def setAppDataEngine(app ,conf_dict):
    if conf_dict['data_engine'] == 'mongodb' :
        args = conf_dict[ conf_dict['data_engine'] ]
        if args['username'] and args['password'] :
            mongourl = 'mongodb://%s:%s@%s:%s/%s' % ( args['username'] ,args['password'],args['host'] ,args['port'] ,args['db'] )
        else:
            mongourl = 'mongodb://%s:%s/%s' % ( args['host'] ,args['port'] ,args['db'] )

        app.mongodb = PyMongo(app, mongourl).db[args['collection']]

        return





if __name__ == "__main__":

    start_web()

    # app.run()
