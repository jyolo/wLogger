from ParserAdapter.BaseAdapter import Adapter
from parse import parse,search,with_pattern
import os,time,importlib,re,logging

class loggerParse(object):


    def __init__(self ,server_type,log_path):
        self.server_type = server_type
        self.log_path = log_path
        self.__handler = self.__findHandlerAdapter(server_type)(log_path)
        self.__format = self.__handler.getLogFormat()
        # 允许的的日志变量
        self.__allow_log_format_vars = self.__getAllowKeysFromHandleFormat()


    @property
    def logFormat(self):
        return self.__format

    @logFormat.setter
    def logFormat(self,value):

        if not isinstance(value, dict):
            raise ValueError('log_format 必须是 dict类型')

        self.__format = []

        if value.__len__() == 0:
            raise ValueError('不允许空字典')

        for item in value:
            if not isinstance(value[item], dict):
                raise ValueError('log_format 列表里面元素必须是字典 示例: %s' % ("[{'$remote_addr': {'desc': '客户端IP' , 'example':'127.0.0.1' }},......]") ,)

            else:
                if item not in self.__allow_log_format_vars:
                    raise ValueError('%s 不在允许日志变量的范围内. 当前可用的日志变量:%s' % (item ,self.__allow_log_format_vars))

                self.__format.append(item)


        return self.__format





    #  动态调用handel方法 and 给出友好的错误提示
    def __getattr__(self, item ,**kwargs):

        if hasattr(self.__handler,item):
            def __callHandleMethod(**kwargs):
                if(list(kwargs.keys()) != ["log_format", "log_line"]) and item == 'parse':
                    raise ValueError('%s handle %s 方法:需要两个kwargs (log_format="", log_line="")' % (self.server_type, item))

                if (list(kwargs.keys()) != ["log_conf", "log_type"]) and item == 'getLogFormatByConfStr':
                    raise ValueError('%s handle %s 方法:需要两个kwargs (log_conf="", log_type="")' % (self.server_type, item))


                return getattr(self.__handler,item)(**kwargs)
        else:
            raise ValueError('%s handle 没有 %s 方法'  % (self.server_type,item) )

        return __callHandleMethod

    def __findHandlerAdapter(self,server_type):
        try:
            handler_module = 'ParserAdapter.%s' % server_type
            return importlib.import_module(handler_module).Handler
        except ModuleNotFoundError:
            raise ValueError('server_type not found')

    def __getAllowKeysFromHandleFormat(self):
        handle_format = self.__format

        allow_vars = []
        for hf in handle_format:

            if not isinstance(handle_format[hf],dict):
                raise ValueError('%s handle 的format 格式有误' % self.server_type)

            allow_vars.append(hf)


        return allow_vars






if __name__ == "__main__":

    obj = loggerParse(
        server_type='Nginx',
        log_path = '/www/wwwlogs/local.test.com.log',

    )

    print(obj.logFormat)
    # print(obj.getLogFormatByConfStr())

    web_conf = '/www/server/nginx/conf/nginx.conf'
    obj.getLoggerFormatByServerConf(web_conf)

    # log_name, log_formater = obj.getLogFormatByConfStr(log_conf=str, log_type='string')

    # 根据用户自定义的字符串 生成 配置项
    # str = "$remote_addr - $request $request_time - $status \n"
    # conf = obj.getLogFormatConfStr(str)
    # print(conf)

    # 根据生成的配置向 设定 匹配标准
#     str = """
#         log_format main	'$remote_addr - $remote_user [$time_local] [$msec] [$request_time] [$http_host] "$request" '
#                           '$status $body_bytes_sent "$request_body" "$http_referer" '
#                           '"$http_user_agent" $http_x_forwarded_for - $connection - $connection_requests - $request_length - $request_time ';
#
#
#     """

    # nginx 默认值
    """
        log_format  combined  '$remote_addr - $remote_user  [$time_local]  '
                       ' "$request"  $status  $body_bytes_sent  '
                       ' "$http_referer"  "$http_user_agent" ';
    """

    '''
        获取解析日志的 log_name , log_formater  
    '''
    # str = conf_list[0]
    # log_type = 'string'
    # log_name,log_formater = obj.getLogFormatByConfStr(log_conf=str,log_type='string')
    # print('---------%s %s' % (log_name,log_formater))
    #
    # '''
    #  读取日志文件分发 到 队列 start
    #  '''
    # obj.startRead(obj.log_path)



