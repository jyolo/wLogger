# coding=UTF-8
from flask import Response,current_app,request,session
from sqlalchemy import text
from webServer.customer import Func,ApiCorsResponse
import json,time,datetime



# 自定义mysql 数据获取class
class MysqlDb():

    @classmethod
    def get_request_num_by_url(cls):

        today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num from {0} 
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': today})


            total = Func.fetchone(res)['total_num']


        with current_app.db.connect() as cursor:
            sql = text("""
                select (count(*)/{0}) as percent,request_url from {1}
                where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                group by request_url
                order by percent desc
                limit 50
                """.format(total,current_app.db_engine_table)
               )

            res = cursor.execute(sql,{'today':today})
            data = Func.fetchall(res)
            data.reverse()
            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_ip(cls):

        today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num,remote_addr from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                       group by remote_addr
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': today})
            data = Func.fetchall(res)
            data.reverse()
            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_urls_by_ip(cls):
        if not request.args.get('ip'):
            return ApiCorsResponse.response('缺少ip参数', False)

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num ,request_url from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today and remote_addr = :remote_addr
                       group by request_url
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )

            ip = request.args.get('ip')
            today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

            res = cursor.execute(sql, {'today': today,'remote_addr':ip})
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_status(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num,`status`  from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today and `status` != 200
                       group by status
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )

            today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

            res = cursor.execute(sql, {'today': today})
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_status_code(cls):

        if not request.args.get('code'):
            return ApiCorsResponse.response('缺少code参数', False)


        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num,`request_url` from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today AND `status` = :status
                       group by request_url
                       order by total_num desc
                       limit 30
                       """.format(current_app.db_engine_table)
                       )

            code = request.args.get('code')
            today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

            res = cursor.execute(sql, {'today': today,'status':code})
            data = Func.fetchall(res)
            data.reverse()
            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_secends(cls):

        if request.args.get('type') == 'init':
            # 　一分钟 * 10 10分钟
            limit = 60 * 10
        else:
            limit = 5

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_request_num,`timestamp` from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today 
                       group by `timestamp`
                       order by `timestamp` desc 
                       limit {1}
                       """.format(current_app.db_engine_table,limit)
                       )


            today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

            res = cursor.execute(sql, {'today': today})
            res = Func.fetchall(res)

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
    def get_request_num_by_province(cls):

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as value,`province` as fullname from {0}
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today 
                       group by `province`
                       order by `value` desc 
                       """.format(current_app.db_engine_table)
                       )

            today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

            res = cursor.execute(sql, {'today': today})
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)