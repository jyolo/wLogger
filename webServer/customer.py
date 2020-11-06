# coding=UTF-8
from flask import Response,current_app
import json


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

