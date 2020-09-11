from ParserAdapter.BaseAdapter import Adapter
import os,inspect,importlib

class loggerParse(object):


    def __init__(self ,server_type,log_path):
        self.server_type = server_type
        self.__handler = self.__findHandlerAdapter(server_type)(log_path)
        self.__format = self.__handler.get_log_format()
        # 允许的的日志变量
        self.__allow_log_format_vars = self.__getAllowKeysFromHandleFormat()

    """
        
    """
    def __getAllowKeysFromHandleFormat(self):
        handle_format = self.__format
        allow_vars = []
        for hf in handle_format:
            if not isinstance(hf,dict):
                raise ValueError('%s handle 的format 格式有误' % self.server_type)

            try:
                allow_vars.append(list(hf.keys())[0])
            except IndexError:
                raise ValueError('%s handle 的format 不允许空字典' % self.server_type)


        return allow_vars

    @property
    def log_format(self):
        return self.__format


    @log_format.setter
    def log_format(self,value):
        if not isinstance(value, list):
            raise ValueError('log_format 必须是 列表类型')

        self.__format = []

        for item in value:
            if not isinstance(item, dict):
                raise ValueError('log_format 列表里面元素必须是字典 示例: %s' % ("[{'$remote_addr': {'desc': '客户端IP' , 'example':'127.0.0.1' }},......]") ,)

            else:
                try:
                    vars = list(item.keys())[0]
                except IndexError:
                    raise ValueError('log_format 列表中不允许空字典')

                if vars not in self.__allow_log_format_vars:
                    raise ValueError('%s 不在允许日志变量的范围内. 当前可用的日志变量:%s' % (vars ,self.__allow_log_format_vars))



                self.__format.append(item)

        return self.__format

    def set_log_format(self):
        print(self.log_format)
        return self.__handler.set_log_format()

        # 获取score




    def getHandler(self):
        return self.__handler


    def __findHandlerAdapter(self,server_type):
        try:
            handler_module = 'ParserAdapter.%s' % server_type
            return importlib.import_module(handler_module).Handler
        except ModuleNotFoundError:
            raise ValueError('server_type not found')






if __name__ == "__main__":

    obj = loggerParse(
        server_type='Nginx',
        log_path = '/www/wwwlogs/local.test.com.log',
    )


    obj.log_format = [
            {'$remote_addr': {'desc': '客户端IP' , 'example':'127.0.0.1' }},
            # {'$http_x_forwarded_for': {'desc': '客户端代理IP多个逗号分割' ,'example':'203.98.182.163, 203.98.182.163'}},
            # {'$request': {'desc': '请求信息' ,'example':'GET /api/server/?size=50&page=1 HTTP/1.1'}},
            # {'$request_body': {'desc': 'post提交的数据' ,'example':'name=xxx&age=18'}},
            # {'$request_length': {'desc': '请求的字节长度' ,'example':'988'}},
            # {'$request_time': {'desc': '请求花费的时间' ,'example':'0.018'}},
            # {'$status': {'desc': '请求状态码' ,'example':'200'}},
            # {'$bytes_sent':{'desc':'发送给客户端的总字节数(包括响应头)' ,'example':'113'} },
            # {'$connection':{'desc':'连接到序列号' ,'example':'26'} },
            # {'$connection_requests':{'desc':'每个连接到序列号的请求次数' ,'example':'3'} },
            # {'$http_referer':{'desc':'请求的来源网址' ,'example':'www.baidu.com'} },
            # {'$http_user_agent':{'desc':'客户端的UA信息' ,'example':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'} },
            # {'$upstream_addr':{'desc':'后端接收请求的服务的ip或地址' ,'example':'unix:/tmp/php-cgi-71.sock'} },
            # {'$time_local':{'desc':'本地时间格式' ,'example':'11/Sep/2020:15:20:43 +0800'} },
        ]

    print(obj.log_format)


