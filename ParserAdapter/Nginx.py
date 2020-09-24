from ParserAdapter.BaseAdapter import Adapter
from parse import parse,search,findall
import os,json,re,time

"""
$remote_addr,$http_x_forwarded_for  #记录客户端IP地址
$remote_user   #记录客户端用户名称

$request  #记录请求的方法 URL和HTTP协议 (包含 $request_method $request_method http协议  比如: 'GET /api/server/?size=10&page=1 HTTP/1.1')
$request_method 请求方法
$request_uri 请求的URL 
$request_length   #请求的长度（包括请求行，请求头和请求正文）。
$request_time #请求处理时间，单位为秒，精度毫秒；从读入客户端的第一个字节开始，直到把最后一个字符发送给客户端后进行日志写入位置。
$upstream_response_time 从Nginx向后端（php-cgi)建立连接开始到接受完数据然后关闭连接为止的时间  ($request_time 包括 $upstream_response_time) 所以如果使用nginx的accesslog查看php程序中哪些接口比较慢的话，记得在log_format中加入$upstream_response_time。

$status        #记录请求状态码
$body_bytes_sent  #发送给客户端的字节数，不包括响应头的大小；该变量与Apache模块mod_log_config李的“%B”参数兼容
$bytes_sent    #发送给客户端的总字节数
$connection    #连接到序列号
$connection_requests #当前通过一个链接获得的请求数量

$msec       #日志写入时间，单位为秒精度是毫秒。
$pipe       #如果请求是通过HTTP流水线(pipelined)发送，pipe值为“p”,否则为".".

$http_referer  #记录从那个页面链接访问过来的
$http_user_agent  #记录客户端浏览器相关信息

$time_iso8601 ISO8601标准格式下的本地时间  2020-09-11T15:01:38+08:00
$time_local  #通用日志格式下的本地时间 11/Sep/2020:15:01:38 +0800

$upstream_addr 服务端响应的地址 
$upstream_http_host  服务端响应的地址 


"""

class Handler(Adapter):



    def __init__(self,*args ,**kwargs):pass


    def getLogFormat(self):
        return {
            '$remote_addr': {'desc': '客户端IP' , 'example':'127.0.0.1' },
            '$remote_user': {'desc': '记录客户端用户名称' , 'example':'client_name' },
            '$http_x_forwarded_for': {'desc': '客户端代理IP多个逗号分割' ,'example':'203.98.182.163, 203.98.182.169' },
            '$request': {'desc': '请求信息' ,'example':'GET /api/server/?size=50&page=1 HTTP/1.1' },
            '$request_method': {'desc': '请求方法' ,'example':'GET' },
            '$scheme':{'desc': '请求协议' ,'example':'HTTP/1.1'  } ,
            '$request_uri': {'desc': '请求链接' ,'example':'/api/server/?size=50&page=1' },
            '$request_body': {'desc': 'post提交的数据' ,'example':'name=xxx&age=18'  },
            '$request_length': {'desc': '请求的字节长度' ,'example':'988' ,'format':'d' },
            '$request_time': {'desc': '请求花费的时间' ,'example':'0.018' ,'format':'g'},
            '$msec': {'desc': '当前的Unix时间戳 (1.3.9, 1.2.6)' ,'example':'' ,'format':'g'},
            '$upstream_response_time': {'desc': 'nginx交给后端cgi响应的时间(小于$request_time)' ,'example':'0.018' ,'format':'g'},
            '$status': {'desc': '请求状态码' ,'example':'200','format':'d'},
            '$bytes_sent':{'desc':'发送给客户端的总字节数(包括响应头)' ,'example':'113' ,'format':'d'} ,
            '$body_bytes_sent':{'desc':'发送给客户端的总字节数(不包括响应头)' ,'example':'266' ,'format':'d'} ,
            '$connection':{'desc':'连接到序列号' ,'example':'26' ,'format':'d'} ,
            '$connection_requests':{'desc':'每个连接到序列号的请求次数' ,'example':'3' ,'format':'d'} ,
            '$host':{'desc':'请求头里的host属性,如果没有就返回 server_name' ,'example':'www.baidu.com'} ,
            '$http_host':{'desc':'请求头里的host属性' ,'example':'www.baidu.com'} ,
            '$http_referer':{'desc':'请求的来源网址' ,'example':'www.baidu.com'} ,
            '$http_user_agent':{'desc':'客户端的UA信息' ,'example':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'} ,
            '$upstream_addr':{'desc':'后端接收请求的服务的ip或地址' ,'example':'unix:/tmp/php-cgi-71.sock'} ,
            '$upstream_http_host':{'desc':'服务端响应的地址 ' ,'example':'unix:/tmp/php-cgi-71.sock'} ,
            '$time_iso8601':{'desc':'iso8601时间格式' ,'example':'2020-09-11T15:20:43+08:00','format':'ti'} ,
            '$time_local':{'desc':'本地时间格式' ,'example':'11/Sep/2020:15:20:43 +0800','format':'th'} ,
        }


    """
        日志解析
    """
    def parse(self,log_format='',log_line=''):
        #  多个空格替换成一个空格
        line = re.sub(r'\s+', ' ', log_line)
        # 处理日志匹配 (耗时操作)
        res = search(log_format, line)
        if (res is None):
            raise ValueError('没有匹配到数据')

        if 'time_local' in res.named:
            res.named['time_local'] = str(res.named['time_local']).replace('+08:00','')


        return res.named


    """
        根据录入的格式化字符串 返回 parse 所需 log_format 配置
    """
    def getLogFormatByConfStr(self ,log_conf='' ,log_type = 'string'):
        if log_type not in ['string','json']:
            raise ValueError('_type 参数类型错误')


        # 去掉换行
        str = log_conf.replace("\n", '')

        # 处理换行后的 引号
        str = re.sub(r'\'\s+\'', '', str)


        if (log_type == 'string'):
            # 获取日志名字后面的 日志格式
            res = re.findall(r'\s?log_format\s+(\w+)\s+\'([\s|\S]?\$\w+[\s|\S]+)+\'', str)

        elif (log_type == 'json'):
            # 获取日志名字后面的 日志格式
            res = re.findall(r'\s?log_format\s+(\w+)\s+\'\{([\'|\"](\w+)[\'|\"]\:[\'|\"](\S+)[\'|\"])+\}\'', str)

        if not res:
            raise ValueError('获取格式化字符串失败')
            return

        # 日志名称
        log_name = res[0][0]
        # 获取到匹配到的 日志格式
        log_format_str = res[0][1].strip()
        format = re.sub(r'(\$\w+)+', self.__replaceLogVars, log_format_str)

        return (log_name ,format)


    def getLoggerFormatByServerConf(self,server_conf_path):


        # 根据配置文件 自动获取 log_format 字符串
        with open(server_conf_path,'rb') as fd:
            content = fd.read().decode(encoding="utf-8")


        res = re.findall(r'log_format\s+\w+\s+\'[\s\S]*\S?\'\S?\;' ,content)

        conf_list = res[0].strip().strip(';').split(';')

        format_list = {}
        for i in conf_list:
            res = re.findall(r'log_format\s+(\w+)\s+',i)
            if len(res):
                format_list[res[0]] = i


        format_list['defualt'] = """ 
            log_format  main '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"  ';
        """

        del content

        return format_list

    """
        找到匹配中的日志变量
    """
    def __replaceLogVars(self,matched):

        try:
            log_format = self.getLogFormat()
            vars = log_format[matched.group()]
        except KeyError :
            raise ValueError('%s 日志变量 handle 里面不存在' % matched.group())

        if('format' in vars and len(vars['format'])):
            s = matched.group().replace('$', '') +':'+ vars['format']
        else:
            s = matched.group().replace('$','')
        return '{%s}' % s




