from flask.blueprints import Blueprint
from flask_pymongo import PyMongo
from flask import render_template,request,flash,session,current_app
from server.customer import ApiCorsResponse
import time




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

@home.route('/get_request_num_by_ip' , methods=['GET'])
def get_request_num_by_ip():

    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    res = current_app.mongo.db.logger.aggregate([
        {'$match':{'time_str':{'$regex':now} } },
        {'$group': {'_id': '$remote_addr' ,'total_num':{'$sum':1} } },
        {'$project':{'ip':'$_id' ,'total_request_num': '$total_num' ,'_id':0} },
        {'$sort': {'total_request_num': -1}},
        # {'$limit':1000}
    ])

    return  ApiCorsResponse.response(list(res))


@home.route('/get_request_num_by_secends' , methods=['GET'])
def get_request_num_by_secends():
    if request.args.get('type') == 'init':
        # 　一分钟 * 10 10分钟
        limit = 60 * 10
    else:
        limit = 5


    res = current_app.mongo.db.logger.aggregate([
        {'$group': {'_id': '$timestamp', 'total_num': {'$sum': 1}}},
        {'$project': {'timestamp': '$_id', 'total_request_num': '$total_num', '_id': 0}},
        {'$sort': {'timestamp': -1}},
        {'$limit': limit}
    ])

    data = []
    for i in res:
        item = {}
        # item['timestamp'] = time.strftime('%H:%M:%S', time.localtime(i['timestamp']))
        # * 1000 for js timestamp
        item['timestamp'] = i['timestamp'] * 1000
        item['total_request_num'] = i['total_request_num']
        data.append(item)

    data.reverse()
    return  ApiCorsResponse.response(data)


if __name__ == "__main__":
    pass