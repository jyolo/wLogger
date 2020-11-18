# coding=UTF-8
from flask import Response,current_app,request,session
from sqlalchemy import text
from webServer.customer import Func
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
                select count(*) as total_num,request_url from {0}
                where FROM_UNIXTIME(`timestamp`,'%Y-%m-%d') = :today
                group by request_url
                order by total_num desc
                """.format(current_app.db_engine_table)
               )


            res = cursor.execute(sql,{'today':today})

            return Func.fetchall(res)




