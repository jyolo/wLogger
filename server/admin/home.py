from flask.blueprints import Blueprint
from flask import render_template ,request,flash,session,current_app
import json

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
    # print(current_app.mongodb.find().count())
    #
    #
    res = current_app.mongodb.aggregate([
        {'$group': {
                '_id': '$remote_addr' ,
                'total_num':{'$sum':1}
            }
        },
        {'$project':{'ip':'$_id' ,'total_request_num': '$total_num' ,'_id':0} },
        {'$sort': {'total_request_num': -1}},
        {'$limit':10}

    ])


    # with res as course:
    #     data = []
    #     for item in course:
    #         data.append(item)

    data = list(res)


    return { 'data': data}


if __name__ == "__main__":
    pass