# coding=UTF-8
from ParserAdapter.BaseAdapter import Adapter,ParseError
from parse import parse,search,findall,compile
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

    unsupport_nickname = ['request',]

    log_line_pattern_dict = {}

    def __init__(self,*args ,**kwargs):
        super(Handler,self).__init__()


    """
    每个日志变量 dict 支持一下key 
    nickname 对该变量名称取别名 ; 默认直接使用变量名称
    re 指定该变量匹配的正则  ; 默认 [\s|\S]+?
    
    mysql_field_type 指定该变量值mysql中的 字段类型
    mysql_key_field True (bool) 默认普通索引 ,指定索引 用字符串 UNIQUE ,FULLTEXT (区分大小写)
        列表的时候 
        '$remote_addr': {
                'nickname':'ip' ,
                'mysql_field_type':'varchar(15)',
                'mysql_key_field': [
                    表示当前字段 和 $time_local 里 extend timestamp 联合索引 key ip_timestamp (ip,timestamp)
                    '$time_local.timestamp', 
                    表示当前字段 和 $time_iso8601 里 extend timestamp 联合索引 key ip_timestamp (ip,timestamp)
                    '$time_iso8601.timestamp',
                    表示当前字段 和 $time_iso8601 里 extend timestamp 联合索引 key ip_status_request_url_method (ip,status,request,url,method)
                    ['$status','$request.request_url','$request.method']
                ],
        }
        
    """
    def getLogFormat(self):

        return {
            ### 非 nginx 日志变量 附加自定义字段
            '$node_id':{
                'mysql_field_type': 'varchar(255)',
                'mysql_key_field' : True
            },
            '$app_name':{
                'mysql_field_type': 'varchar(255)',
            },
            ### 非 nginx 日志变量 附加自定义字段


            # 客户端IP example 127.0.0.1
            '$remote_addr': {
                'nickname':'ip' ,
                'mysql_field_type':'varchar(15)',
                'mysql_key_field': [
                    '$time_local.timestamp',
                    '$time_iso8601.timestamp',
                    ['$status','$request.request_url','$request.request_method']
                ],
                'extend_field':{
                    'isp':{
                        'mysql_field_type': 'varchar(30)',
                    },
                    'city':{
                        'mysql_field_type': 'varchar(30)',
                        'mysql_key_field': ['$status'],
                    },
                    'city_id':{
                        'mysql_field_type': 'int(10)',
                    },
                    'province':{
                        'mysql_field_type': 'varchar(30)',
                        'mysql_key_field': [
                            '$time_local.timestamp',
                            '$time_iso8601.timestamp',
                        ],
                    },
                    'country':{
                        'mysql_field_type': 'varchar(30)',
                    }
                }
            },
            # 请求信息 example GET /api/server/?size=50&page=1 HTTP/1.1
            '$request': {
                'extend_field': {
                    'request_method': {
                        'mysql_field_type': 'varchar(10)',
                        'mysql_key_field':True,
                    },
                    'request_url': {
                        'mysql_field_type': 'varchar(255)',
                        'mysql_key_field': ['$time_local.timestamp', '$time_iso8601.timestamp'],
                    },
                    'args': {
                        'mysql_field_type': 'text',
                    },
                    'server_protocol': {
                        'mysql_field_type': 'varchar(10)',
                    },
                }
            },
            # 记录客户端用户名称 example client_name
            '$remote_user': { },
            # 客户端代理IP多个逗号分割 example 203.98.182.163, 203.98.182.169
            '$http_x_forwarded_for': {
                'nickname':'proxy_ip',
                'mysql_field_type':'varchar(15)'
            },
            # 请求方法 example GET
            '$request_method': {
                'mysql_field_type': 'varchar(10)',
            },
            # 请求协议 example HTTP/1.1
            '$scheme':{
                'mysql_field_type': 'varchar(255)'
            } ,
            # 服务器的HTTP版本 example “HTTP/1.0” 或 “HTTP/1.1”
            '$server_protocol':{
                'mysql_field_type': 'varchar(10)'
            } ,
            # 请求链接 example /api/server/?size=50&page=1
            '$request_uri': {
                'nickname':'request_url',
                'mysql_field_type': 'varchar(255)',
                'mysql_key_field': ['$time_local.timestamp', '$time_iso8601.timestamp'],
            },
            # post提交的数据 example name=xxx&age=18
            '$request_body': {
                'mysql_field_type': 'mediumtext'
            },
            # 请求的字节长度 example 988
            '$request_length': {
                're': '\d+?' ,
                'type':'int',

            },
            # 请求花费的时间 example 0.018
            '$request_time': {
                'type':'float',
                'mysql_field_type':'float(10, 3)',
            },
            # 当前的Unix时间戳 nginx 版本需大于 (1.3.9, 1.2.6)
            '$msec': {
                'mysql_field_type': 'int(16)',
            },
            # nginx交给后端cgi响应的时间(小于$request_time) example 0.215 或者 0.215, 0.838
            '$upstream_response_time': {
               'mysql_field_type': 'varchar(50)',
            },
            # 请求状态码 example 200
            '$status': {
                're': '\d*?',
                'nickname':'status_code',
                'mysql_field_type': 'int(4)',
                'mysql_key_field': ['$time_local.timestamp','$time_iso8601.timestamp'],
            },
            # 发送给客户端的总字节数 (包括响应头) example 113
            '$bytes_sent':{
                're': '\d*?' ,
                'type':'int',
                'mysql_field_type': 'int(4)',
            } ,
            # 发送给客户端的总字节数(不包括响应头) example 266
            '$body_bytes_sent':{
                're': '\d*?' ,
                'type':'int',
                'mysql_field_type': 'int(4)',
            } ,
            # 连接到序列号 example 26
            '$connection':{
                're': '\d*?' ,
                'type':'int',
                'mysql_field_type': 'int(10)',
            } ,
            # 每个连接到序列号的请求次数  example 3
            '$connection_requests':{
                're': '\d*?',
                'type':'int',
                'mysql_field_type': 'int(10)',
            } ,
            # 请求头里的host属性,如果没有就返回 server_name example www.baidu.com
            '$host':{
                'mysql_field_type': 'varchar(255)',
            } ,
            # 请求头里的host属性 example www.baidu.com
            '$http_host':{
                'mysql_field_type': 'varchar(255)',
            } ,
            # 请求的来源网址 example www.baidu.com
            '$http_referer':{
                'mysql_field_type': 'varchar(255)',

            } ,
            # 客户端的UA信息 example Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36
            '$http_user_agent':{
                'nickname':'ua',
                'mysql_field_type': 'mediumtext',
                'mysql_key_field': 'FULLTEXT'

            } ,
            # 后端接收请求的服务的ip或地址 example unix:/tmp/php-cgi-71.sock 负载均衡中 是 192.168.0.122:80, 192.168.0.133:80
            '$upstream_addr':{
                'mysql_field_type': 'varchar(255)',
            }  ,
            # 服务端响应的地址 unix:/tmp/php-cgi-71.sock 同上 是域名
            '$upstream_http_host':{
                'mysql_field_type': 'varchar(255)',
            } ,
            # iso8601时间格式 example 2020-09-11T15:20:43+08:00
            '$time_iso8601':{
                'extend_field': {
                    'time_str': {
                        'mysql_field_type': 'datetime',
                        'mysql_key_field': True
                    },
                    'timestamp': {
                        'mysql_field_type': 'int(10)',
                        'mysql_key_field': True
                    },
                }
            } ,
            # 本地时间格式 example 11/Sep/2020:15:20:43 +0800
            '$time_local':{
                'extend_field': {
                    'time_str': {
                        'mysql_field_type': 'datetime',
                        'mysql_key_field': True
                    },
                    'timestamp': {
                        'mysql_field_type': 'int(10)',
                        'mysql_key_field': True
                    },
                }
            } ,
        }


    def parse_request_url(self, data):

        try:
            if 'request' in data:
                _strarr = data['request'].split(' ')

                data['request_method'] = _strarr[0]
                _url = _strarr[1].split('?')

                if len(_url) > 1:
                    data['request_url'] = _url[0]
                    data['args'] = _url[1]
                else:
                    data['request_url'] = _url[0]
                    data['args'] = ''

                data['server_protocol'] = _strarr[2]

                del data['request']

            if 'request_uri' in data:
                _strarr = data['request_uri'].split('?')

                data['request_url'] = _url[0]
                data['args'] = _url[1]

                del data['request_uri']
        except IndexError as e:
            raise ValueError('解析日志 request_url 错误;data : %s' % json.dumps(data))

        return data



    """
        日志解析
    """
    def parse(self,log_format_name='',log_line=''):


        log_format_list = self.log_line_pattern_dict[log_format_name]['log_format_list']
        log_format_recompile = self.log_line_pattern_dict[log_format_name]['log_format_recompile']

        res = log_format_recompile.match(log_line)

        if res == None:
            raise ParseError('解析日志失败,请检查client 配置中 日志的 格式名称是否一致 log_format_name')

        matched = list(res.groups())

        if len(matched) == len(log_format_list):
            data = {}
            del_key_name = []
            field_type_map = {}

            for i in range(len(list(log_format_list))):
                key_name = log_format_list[i]

                # request 参数不支持别名
                if 'nickname' in self.getLogFormat()['$' + log_format_list[i]] :
                    key_name = self.getLogFormat()['$' + log_format_list[i]]['nickname']

                # 解析 ip 对应的 地理位置信息
                if log_format_list[i] == 'remote_addr':
                    ip_data = self.parse_ip_to_area(matched[i])
                    data.update(ip_data)

                # 解析 request 成 request_method ,request_url ,args ,server_protocol ,
                if log_format_list[i] == 'request':
                    request_extend_data = self.parse_request_to_extend(matched[i])
                    data.update(request_extend_data)
                    del_key_name.append(key_name)

                # 解析 time_iso8601 成 timestr , timestamp
                if log_format_list[i] == 'time_local':
                    time_data = self.parse_time_to_str(log_format_list[i],matched[i])
                    data.update(time_data)
                    del_key_name.append(key_name)

                data[key_name] = matched[i]


        # 剔除掉 解析出拓展字符串的 字段
        for i in del_key_name:
            del data[i]


        return data


    """
        根据录入的格式化字符串 返回 parse 所需 log_format 配置
    """
    def getLogFormatByConfStr(self ,log_format_conf ,log_format_name ,log_type):
        if log_type not in ['string','json']:
            raise ValueError('_type 参数类型错误')

        # 日志格式不存在 则 预编译
        if log_format_name not in self.log_line_pattern_dict:

            # 去掉换行
            str = log_format_conf.replace("\n", '')

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
            log_format_str = res[0][1].strip().replace('[', '\[').replace(']', '\]')

            log_format_list = re.findall(r'(\w+)',log_format_str)

            format = re.sub(r'(\$\w+)+', self.__replaceLogVars, log_format_str).strip()

            self.log_line_pattern_dict[log_format_name] = {
                'log_format_list':log_format_list ,
                'log_format_recompile':re.compile(format ,re.I)
            }



    """
        找到匹配中的日志变量
    """
    def __replaceLogVars(self,matched):


        s = matched.group()

        if s not in self.getLogFormat():
            raise ValueError('handle 里面不存在日志变量:%s' % s)

        if 're' in self.getLogFormat()[s]:
            re_str = self.getLogFormat()[s]['re']
        else:
            re_str = '[\s|\S]*?'

        return '(%s)' % re_str



    def getLoggerFormatByServerConf(self,server_conf_path):


        # 根据配置文件 自动获取 log_format 字符串
        with open(server_conf_path,'rb') as fd:
            content = fd.read().decode(encoding="utf-8")

        format_list = {}
        format_list['defualt'] = """
                    log_format  main '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"  ';
                """

        res = re.findall(r'log_format\s+\w+\s+\'[\s\S]*\S?\'\S?\;' ,content)

        if len(res) == 0:
            return format_list


        conf_list = res[0].strip().strip(';').split(';')
        for i in conf_list:
            res = re.findall(r'log_format\s+(\w+)\s+',i)
            if len(res):
                format_list[res[0]] = i


        del content

        return format_list







