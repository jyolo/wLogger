from flask import Flask
from flask_pymongo import PyMongo
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from webServer.admin.user import user
from webServer.admin.home import home
from webServer.divers.mysql import MysqlDb
from webServer.divers.mongo import MongoDb
from webServer.customer import Func


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



    app.env = conf_dict['env']
    if conf_dict['debug'] == 'True':
        app.debug = True
    elif conf_dict['debug'] == 'False':
        app.debug = False

    app.secret_key = conf_dict['secret_key']

    app.config.from_mapping(conf_dict)
    app.register_blueprint(home, url_prefix='/')
    app.register_blueprint(user, url_prefix='/user')

    # init flask db engine
    setAppDataEngine(conf_dict)

    app.run()



def setAppDataEngine(conf_dict):
    args = conf_dict[conf_dict['data_engine']]
    db_engine_table = Func.getTableName(args ,data_engine = conf_dict['data_engine'])

    if conf_dict['data_engine'] == 'mongodb':
        if args['username'] and args['password']:
            mongourl = 'mongodb://%s:%s@%s:%s/%s' % (
            args['username'], args['password'], args['host'], args['port'], args['db'])
        else:
            mongourl = 'mongodb://%s:%s/%s' % (args['host'], args['port'], args['db'])

        app.db = PyMongo(app,mongourl).db
        app.dbClass = MongoDb
        app.db_engine_table = db_engine_table

    if conf_dict['data_engine'] == 'mysql':
        from flask_sqlalchemy import SQLAlchemy
        from sqlalchemy import create_engine
        # from sqlalchemy.engine.result
        import pymysql

        #sqlalchemy docs https://docs.sqlalchemy.org/en/13/core/pooling.html
        pymysql.install_as_MySQLdb()

        sql_url = 'mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' % (args['username'],args['password'],args['host'],args['port'],args['db'])


        app.config['SQLALCHEMY_DATABASE_URI'] = sql_url
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
        # # app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
        # app.config['MYSQL_CURSORCLASS'] = pymysql.cursors.DictCursor

        db = SQLAlchemy(app)
        app.db = db.engine
        app.dbClass = MysqlDb
        app.db_engine_table = db_engine_table




def CustomerResponse(data):
    return '123'


if __name__ == "__main__":

    pass

    # app.run()
