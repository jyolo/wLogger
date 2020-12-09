# coding=UTF-8
from flask import current_app,request,session
from webServer.customer import Func,ApiCorsResponse
import time,re




class MongoDb():

    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    @classmethod
    def get_total_ip(cls):
        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}}},
            {'$group': {'_id': '$ip'}},
            {'$group': {'_id': '','total_num':{'$sum':1} }},
            {'$project': { '_id': 0}},
            {'$sort': {'total_num': -1}},
        ])
        res = list(res)
        if len(res) == 0 :
            return ApiCorsResponse.response({})
        return ApiCorsResponse.response(res[0])

    @classmethod
    def get_total_pv(cls):
        res = current_app.db[current_app.db_engine_table].find({'time_str': {'$regex': '^%s' % cls.today}}).count()
        return ApiCorsResponse.response({'total_num':res})

    @classmethod
    def get_request_num_by_url(cls):
        # if request.args.get('type') == 'init':
        #     # 　一分钟 * 10 10分钟
        #     limit = 60 * 10
        # else:
        #     limit = 5


        collection = current_app.db_engine_table


        # total = current_app.db[collection].find({'time_str': {'$regex': '^%s' % cls.today}}).count()

        res = current_app.db[collection].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}}},
            {'$group': {'_id': '$request_url', 'total_num': {'$sum': 1}}},
            {'$project': {
                'request_url': '$_id',
                'total_num': 1,
                '_id': 0,
                # 'percent': {'$toDouble': {'$substr': [{'$multiply': [{'$divide': ['$total_num', total]}, 100]}, 0, 4]}}
            }
            },
            {'$sort': {'total_num': -1}},
            {'$limit': 50}
        ])

        data = list(res)
        data.reverse()

        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_ip(cls):
        if request.args.get('type') == 'init':
            # 　一分钟 * 10 10分钟
            limit = 60 * 10
        else:
            limit = 5

        session['now_timestamp'] = int(time.time())


        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}}},
            {'$group': {'_id': '$ip', 'total_num': {'$sum': 1}}},
            {'$project': {
                'ip':'$_id',
                'total_num': 1,
                '_id':0
                # 'percent':{ '$toDouble': {'$substr':[  {'$multiply':[ {'$divide':['$total_num' , total]} ,100] }  ,0,4  ] }   }
            }
            },
            {'$sort': {'total_num': -1}},
            {'$limit': 50}
        ])



        data = list(res)
        data.reverse()
        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_urls_by_ip(cls):
        if not request.args.get('ip'):
            return ApiCorsResponse.response('缺少ip参数', False)

        session['now_timestamp'] = int(time.time())

        collection = current_app.db_engine_table



        res = current_app.db[collection].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}, 'ip': request.args.get('ip')}},
            {'$group': {'_id': '$request_url', 'total_num': {'$sum': 1}}},
            {'$project': {
                'total_num': 1,
                'request_url': '$_id',
            }
            },
            {'$sort': {'total_num': -1}},
            {'$limit': 20}
        ])


        data = list(res)
        data.reverse()
        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_status(cls):
        session['now_timestamp'] = int(time.time())



        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}, 'status_code': {'$ne': '200'}}},
            {'$group': {'_id': '$status_code', 'total_num': {'$sum': 1}}},
            {'$project': {'status_code': '$_id', 'total_num': 1, '_id': 0}},
            {'$sort': {'total_num': -1}},
        ])

        data = list(res)
        data.reverse()

        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_status_code(cls):
        if not request.args.get('code'):
            return ApiCorsResponse.response('缺少code参数', False)

        session['now_timestamp'] = int(time.time())



        arg = re.findall('\d+?', request.args.get('code'))
        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}, 'status': ''.join(arg)}},
            {'$group': {'_id': '$request_url', 'total_num': {'$sum': 1}}},
            {'$project': {'request_url': '$_id', 'total_num': 1, '_id': 0}},
            {'$sort': {'total_num': -1}},
        ])

        data = list(res)
        data.reverse()

        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_secends(cls):
        if request.args.get('type') == 'init':
            # 　一分钟 * 10 10分钟
            limit = 60 * 10
        else:
            limit = 5


        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}}},
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
        return ApiCorsResponse.response(data)

    @classmethod
    def get_network_traffic_by_minute(cls):
        current_hour = time.strftime('%Y-%m-%d %H', time.localtime(time.time()))

        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % current_hour}}},
            {'$project': {
                '_id': 0,
                'time_minute': {'$dateToString': {
                    'format': '%Y-%m-%d %H:%M', 'date': {'$dateFromString': {'dateString': '$time_str'}}
                }
                },
                'request_length': {'$toInt': '$request_length'},
                'bytes_sent': {'$toInt': '$bytes_sent'}
            },
            },
            {'$group': {
                '_id': '$time_minute',
                'in_network_sum': {'$sum': '$request_length'},
                'out_network_sum': {'$sum': '$bytes_sent'}
            }
            },
            {'$project': {'_id': 0, 'time_str': '$_id', 'in_network': {'$divide': ['$in_network_sum', 1024]},
                          'out_network': {'$divide': ['$out_network_sum', 1024]}}},
            {'$sort': {'time_str': -1}},

        ])

        data = []
        for i in res:
            i['time_str'] = int(time.mktime(time.strptime(i['time_str'], '%Y-%m-%d %H:%M')))
            data.append(i)

        data.reverse()

        return ApiCorsResponse.response(data)

    @classmethod
    def get_ip_pv_num_by_minute(cls):

        current_hour = time.strftime('%Y-%m-%d %H', time.localtime(time.time()))

        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % current_hour}}},
            {'$project': {
                    '_id': 0,
                    'time_minute': {
                            '$dateToString': {
                                'format': '%Y-%m-%d %H:%M', 'date': {'$dateFromString': {'dateString': '$time_str'}}
                            }
                    },
                    'ip': 1
                },
            },
            {'$group': {
                '_id': '$time_minute',
                'pv_num': {'$sum': 1},
                'ip_arr': {'$addToSet': '$ip'}
                }
            },
            {'$project': {'_id': 0, 'time_str': '$_id', 'pv_num': 1, 'ip_num': {'$size': '$ip_arr'}}},
            {'$sort': {'time_str': -1}},
        ])

        data = []
        for i in res:
            i['time_str'] = int(time.mktime(time.strptime(i['time_str'], '%Y-%m-%d %H:%M')))
            data.append(i)

        data.reverse()
        return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_province(cls):
        session['now_timestamp'] = int(time.time())

        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {'time_str': {'$regex': '^%s' % cls.today}}},
            {'$group': {'_id': '$province', 'total_num': {'$sum': 1}}},
            {'$project': {'province': '$_id', 'value': '$total_num', '_id': 0}},
            {'$sort': {'total_num': -1}},
        ])

        data = list(res)
        data.reverse()

        return ApiCorsResponse.response(data)

    @classmethod
    def get_spider_by_ua(cls):
        res = current_app.db[current_app.db_engine_table].aggregate([
            {'$match': {
                'time_str': {'$regex': '^%s' % cls.today} ,
                'ua':{'$regex':'spider'}
                }
            },
            {'$group': {'_id': '$ua', 'total_num': {'$sum': 1}}},
            {'$project': {'ua': '$_id', 'total_num': 1, '_id': 0}},
            {'$sort': {'total_num': -1}},
        ])

        data = list(res)
        data.reverse()
        return ApiCorsResponse.response(data)
