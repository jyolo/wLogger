# coding=UTF-8
from flask import Response,current_app
from sqlalchemy import text
import json,time,datetime


# 自定义跨域 接口 返回 class
class ApiCorsResponse():

    @staticmethod
    def response(data ,success = True,status_code= 200):
        if success :
            re_data = {'msg':'ok','data':data}
        else:
            re_data = {'msg': 'fail', 'error_info': data}

        rep = Response(
            response=json.dumps(re_data) + "\n" ,
            status=status_code,
            mimetype= current_app.config["JSONIFY_MIMETYPE"]
        )

        rep.access_control_allow_origin = '*'
        rep.access_control_allow_methods = ['GET','POST','OPTIONS','PUT','DELETE']

        return rep

class Func():

    save_engine_log_split = ['day', 'week', 'month', 'year']
    @classmethod
    def getTableName(cls,conf):
        table_suffix = ''

        try:
            table = conf['table']
        except KeyError as e:
            raise Exception('配置错误: %s not exists' % e.args)


        if 'save_engine_log_split' in conf:

            if conf['save_engine_log_split'] not in cls.save_engine_log_split:
                raise Exception('webserver 配置项 save_engine_log_split 只支持 %s 选项' % ','.join(cls.save_engine_log_split))

            if conf['save_engine_log_split'] == 'day':

                table_suffix = time.strftime('%Y_%m_%d', time.localtime())

            elif conf['save_engine_log_split'] == 'week':

                now = datetime.datetime.now()
                this_week_start = now - datetime.timedelta(days=now.weekday())
                this_week_end = now + datetime.timedelta(days=6 - now.weekday())

                table_suffix = datetime.datetime.strftime(this_week_start, '%Y_%m_%d') + datetime.datetime.strftime(
                    this_week_end, '_%d')

            elif conf['save_engine_log_split'] == 'month':

                table_suffix = time.strftime('%Y_%m', time.localtime())
            elif conf['save_engine_log_split'] == 'year':

                table_suffix = time.strftime('%Y', time.localtime())

        if len(table_suffix):
            table = table + '_' + table_suffix


        return table
    @classmethod
    def fetchone(cls,resultObj):
        _l = list(resultObj)

        if list(_l) == 0:
            return False

        return _l[0][0]

    @classmethod
    def fetchall(cls, resultObj):
        _list = list(resultObj)
        if len(_list) == 0:
            return False

        return _list

class MysqlDb():

    @classmethod
    def get_request_num_by_url(cls,app):

        today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

        with app.mysql.connect() as cursor:
            sql = text("""
                       select count(*) as total_num from {0} 
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                       """.format(app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': today})

            total = Func.fetchone(res)


        with app.mysql.connect() as cursor:
            sql = text("""
                select count(*) as total_num,request_url from {0}
                where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                group by request_url
                order by total_num desc
                """.format(app.db_engine_table)
               )


            res = cursor.execute(sql,{'today':today})
            print(total)
            print(Func.fetchall(res))




        # total = app.mongo.db.logger.find({'time_str': {'$regex': '^%s' % today}}).count()
        #
        # res = app.mongo.db.logger.aggregate([
        #     {'$match': {'time_str': {'$regex': '^%s' % today}}},
        #     {'$group': {'_id': '$request_url', 'total_num': {'$sum': 1}}},
        #     {'$project': {
        #         'ip': '$_id',
        #         'total_num': 1,
        #         '_id': 0,
        #         'percent': {'$toDouble': {'$substr': [{'$multiply': [{'$divide': ['$total_num', total]}, 100]}, 0, 4]}}
        #     }
        #     },
        #     {'$sort': {'total_num': -1}},
        #     {'$limit': 50}
        # ])
        #
        # data = list(res)
        # data.reverse()
        return {}


class MongoDb():
    @classmethod
    def get_request_num_by_url(cls):
        print('MongoDb')
        pass