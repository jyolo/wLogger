from ParserAdapter.BaseAdapter import Adapter
import os,inspect,importlib,re,logging

class loggerParse(object):


    def __init__(self ,server_type,log_path):
        self.server_type = server_type
        self.__handler = self.__findHandlerAdapter(server_type)(log_path)
        self.__format = self.__handler.getLogFormat()
        # 允许的的日志变量
        self.__allow_log_format_vars = self.__getAllowKeysFromHandleFormat()

    """
        
    """

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
        根据录入的格式化字符串 返回 配置
    """
    def getLogFormatConfStr(self ,_str ,_name = 'main' ,_type = 'string'):
        if _type not in ['string','json']:
            raise ValueError('_type 参数类型错误')

        if _type == 'string':
            return "log_format %s '%s'" % (_name,_str)



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


    def aa(self,matched):
        try:
            print(self.logFormat[matched.group()])
        except KeyError :
            logging.error('%s 日志变量 handle 里面不存在' % matched.group())


        s = matched.group().replace('$','')
        return '{%s:}' % s


if __name__ == "__main__":

    obj = loggerParse(
        server_type='Nginx',
        log_path = '/www/wwwlogs/local.test.com.log',
    )


    # 根据用户自定义的字符串 生成 配置项
    # str = "$remote_addr - $request $request_time - $status \n"
    # conf = obj.getLogFormatConfStr(str)
    # print(conf)

    # 根据生成的配置向 设定 匹配标准
    str = """
        	log_format custom '[$remote_addr] [$http_x_forwarded_for] [$remote_user] [$request_method] [$request_uri] [$request_body] [$request_length] [$request_time] [$upstream_response_time] [$status] [$body_bytes_sent] [$bytes_sent] [$connection] [$connection_requests] [$http_referer] [$http_user_agent] [$time_iso8601]  [$time_local] [$upstream_addr] ';



    """
    str = str.replace("\n", '')
    print(str)
    str = re.sub(r'\'\s+\'','',str)

    res = re.findall(r'\s+log_format\s+(\w+)\s+\'([\s|\S]?\$\w+[\s|\S]+)+\'',str)
    print(res)
    # log_name = res[0][0]
    log_format_str = res[0][1].strip()
    print(log_format_str)


    print(re.sub(r'(\$\w+)+',obj.aa,log_format_str))



