# coding=UTF-8
from flask import Response,current_app,request,session
from sqlalchemy import text
from decimal import Decimal
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
# 自定义函数class
class Func():

    save_engine_log_split = ['day', 'week', 'month', 'year']
    @classmethod
    def getTableName(cls,conf,data_engine):
        table_suffix = ''

        try:
            if data_engine == 'mysql':
                table = conf['table']
            if data_engine == 'mongodb':
                table = conf['collection']

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
        return cls.fetchall(resultObj)[0]

    @classmethod
    def fetchall(cls, resultObj):
        _list = []
        for i in resultObj:
            _dict = {}
            item = i.items()

            for j in item:
                if isinstance(j[1],Decimal):
                    vl = float(Decimal(j[1]).quantize(Decimal('.001')))
                    _dict[j[0]] = vl
                else:
                    _dict[j[0]] = j[1]

            _list.append(_dict)

        return _list






