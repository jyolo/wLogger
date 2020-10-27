from flask.blueprints import Blueprint
from flask_pymongo import PyMongo
from flask import render_template ,request,flash,session,current_app
import sys,time,os



home = Blueprint('home',__name__)

@home.route('/',methods=['GET','POST'])
def index():
    # flash('You were successfully logged in asdasd')
    if(request.method == 'GET'):
        print(request.args.get('var1'))
        return render_template('home/index.html')

    if(request.method == 'POST'):
        var1 = request.form['username']
        var2 = request.form['password']
        print(request.args )
        return {
            'username':var1,
            'password':var2,
            'asd':'asd',
            'sdfsdf':'asdfsdfsdsd'

        }

@home.route('/get_ip_info' , methods=['GET'])
def get_ip_info():

    res = current_app.mongo.db.logger.aggregate([
        {'$group': {'_id': '$remote_addr' ,'total_num':{'$sum':1} } },
        {'$project':{'ip':'$_id' ,'total_request_num': '$total_num' ,'_id':0} },
        {'$sort': {'total_request_num': -1}},
        {'$limit':10}
    ])


    data = list(res)


    return { 'data': data}

@home.route('/get_request_num_by_secends' , methods=['POST'])
def get_request_num_by_secends():

    res = current_app.mongo.db.logger.aggregate([
        {'$group': {'_id': '$timestamp', 'total_num': {'$sum': 1}}},
        # {'$project': {'timestamp': '$_id', 'total_request_num': '$total_num', '_id': 0}},
        {'$sort': {'_id': -1}},
        {'$limit': 10}
    ])

    return {'data': list(res)}

if __name__ == "__main__":
    pass