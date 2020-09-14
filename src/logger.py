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

    """
        动态调用handel方法 and 给出友好的错误提示
    """
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




def startReadLog():
    pass



if __name__ == "__main__":

    obj = loggerParse(
        server_type='Nginx',
        log_path = '/www/wwwlogs/local.test.com.log',

    )
    web_conf = '/www/server/nginx/conf/nginx.conf'

    # 根据配置文件 自动获取 log_format 字符串
    with open(web_conf,'rb') as fd:
        content = fd.read().decode(encoding="utf-8")


    res = re.findall(r'log_format\s+\w+\s+\'[\s\S]*\S?\'\S?\;' ,content)

    conf_list = res[0].strip().strip(';').split(';')

    print('自动获取到 %s 个日志格式配置' % len(conf_list))

    str = conf_list[0]
    # print(conf_list[0])
    # print(conf_list[1])
    # print(conf_list[2])
    # print(conf_list[3])
    #
    # exit()




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

    '''
    读取日志文件分发 到 队列 start
    '''
    str = conf_list[0]


    log_type = 'string'

    log_name,log_format = obj.getLogFormatByConfStr(log_conf=str,log_type='string')

    print('---------%s %s' % (log_name,log_format))

    postion = 0
    with open(obj.log_path,'rb') as fd:
        
        while True:
            if(postion > 0):
                print('------------read position : %s-------------' % postion)
                fd.seek(postion)

            start_time = time.time()
            print('read position : %s' % postion)

            for line in fd:

                line = line.decode(encoding='utf-8')
                if(len(line) == 0 ):
                    continue

                postion = fd.tell()

                # if (log_type == 'string'):
                #     # res = obj.parse(log_format=log_format, log_line=line)
                #     print(line)
                # elif (log_type == 'json'):
                #     print(line)


                # todo 数据上报
                print(line)


            end_time = time.time();
            print(print('耗时: %s' % (end_time - start_time) ))
            time.sleep(1)



