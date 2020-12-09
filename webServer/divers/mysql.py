# coding=UTF-8
from flask import Response,current_app,request,session
from sqlalchemy import text
from webServer.customer import Func,ApiCorsResponse
import json,time,datetime



# 自定义mysql 数据获取class
class MysqlDb():

    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    @classmethod
    def get_total_ip(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(DISTINCT ip) as total_num from {0} 
                       where `timestamp` >= UNIX_TIMESTAMP(:today)
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})

            total = Func.fetchone(res)
            return ApiCorsResponse.response(total)

    @classmethod
    def get_total_pv(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num from {0}  FORCE INDEX(timestamp)
                       where `timestamp` >= UNIX_TIMESTAMP(:today)
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})

            total = Func.fetchone(res)
            return ApiCorsResponse.response(total)

    @classmethod
    def get_request_num_by_url(cls):


        with current_app.db.connect() as cursor:
            sql = text("""
                select count(*) as total_num,request_url from {0} 
                where `timestamp` >= UNIX_TIMESTAMP(:today)
                group by request_url
                order by total_num desc
                limit 10
                """.format(current_app.db_engine_table)
               )

            res = cursor.execute(sql,{'today':cls.today})
            data = Func.fetchall(res)
            data.reverse()
            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_ip(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num,ip from {0}
                       where `timestamp` >= UNIX_TIMESTAMP(:today)
                       group by ip
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})
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
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today and ip = :ip
                       group by request_url
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )

            ip = request.args.get('ip')


            res = cursor.execute(sql, {'today': cls.today,'ip':ip})
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_status(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as total_num,`status_code`  from {0}
                       where `timestamp` >= UNIX_TIMESTAMP(:today) and `status_code` != 200
                       group by status_code
                       order by total_num desc
                       limit 50
                       """.format(current_app.db_engine_table)
                       )



            res = cursor.execute(sql, {'today': cls.today})
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
                       where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today AND `status_code` = :status_code
                       group by request_url
                       order by total_num desc
                       limit 30
                       """.format(current_app.db_engine_table)
                       )

            code = request.args.get('code')


            res = cursor.execute(sql, {'today': cls.today,'status_code':code})
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


            res = cursor.execute(sql, {'today': cls.today})
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
    def get_network_traffic_by_minute(self):
        with current_app.db.connect() as cursor:
            current_hour_str = time.strftime('%Y-%m-%d %H', time.localtime(time.time()))
            next_hour_str = time.strftime('%Y-%m-%d %H', time.localtime(time.time() + 3600))
            current_hour = int(time.mktime(time.strptime(current_hour_str, '%Y-%m-%d %H')))
            next_hour = int(time.mktime(time.strptime(next_hour_str, '%Y-%m-%d %H')))

            sql = text("""
            select round((sum(bytes_sent) /1024),2)  as out_network, round((sum(request_length) / 1024) ,2) as in_network  ,unix_timestamp(STR_TO_DATE(time_str,'%Y-%m-%d %H:%i')) as time_str
            from {0}
            where `timestamp` >= {1} and `timestamp` < {2}
            GROUP BY MINUTE(time_str)
            ORDER BY MINUTE(time_str) desc
            limit 10
            """.format(current_app.db_engine_table, current_hour, next_hour)
                       )

            res = cursor.execute(sql)
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)

    @classmethod
    def get_ip_pv_num_by_minute(cls):
        with current_app.db.connect() as cursor:
            current_hour_str = time.strftime('%Y-%m-%d %H', time.localtime(time.time()))
            next_hour_str = time.strftime('%Y-%m-%d %H', time.localtime(time.time() + 3600))
            current_hour = int(time.mktime(time.strptime(current_hour_str, '%Y-%m-%d %H')))
            next_hour = int(time.mktime(time.strptime(next_hour_str, '%Y-%m-%d %H')))

            sql = text("""
            select count(DISTINCT ip) as ip_num,count(*) as pv_num  ,unix_timestamp(STR_TO_DATE(time_str,'%Y-%m-%d %H:%i')) as time_str
            from {0}
            where `timestamp` >= {1} and `timestamp` < {2}
            GROUP BY MINUTE(time_str)
            ORDER BY MINUTE(time_str) desc
            limit 10
            """.format(current_app.db_engine_table ,current_hour ,next_hour )
                       )


            res = cursor.execute(sql)
            data = Func.fetchall(res)
            data.reverse()
            return ApiCorsResponse.response(data)

    @classmethod
    def get_request_num_by_province(cls):

        with current_app.db.connect() as cursor:
            sql = text("""
                       select count(*) as value,`province`  from {0}
                       where `timestamp` >= UNIX_TIMESTAMP(:today) AND province != '0'
                       group by `province`
                       order by `value` desc 
                       """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})
            data = Func.fetchall(res)
            data.reverse()

            return ApiCorsResponse.response(data)

    @classmethod
    def get_spider_by_ua(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                            select count(*) as total_num ,ua from {0}  
                            where MATCH(`ua`) AGAINST('spider') 
                            GROUP BY ua
                            ORDER BY total_num desc
                              """.format(current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})
            data = Func.fetchall(res)
            return ApiCorsResponse.response(data)

    @classmethod
    def get_device_type_by_ua(cls):
        with current_app.db.connect() as cursor:
            sql = text("""
                            select count(DISTINCT ua) as pc_num, (
			                        select count(DISTINCT ua) as mobile_num
				                    from {0}
				                    where match(`ua`) AGAINST('mobile xfb' IN BOOLEAN MODE)
	                            ) as mobile_num
                            from {1}
                            where match(`ua`) AGAINST('+gecko -mobile' IN BOOLEAN MODE)
                              """.format(current_app.db_engine_table,current_app.db_engine_table)
                       )

            res = cursor.execute(sql, {'today': cls.today})
            data = Func.fetchall(res)
            return ApiCorsResponse.response(data)
