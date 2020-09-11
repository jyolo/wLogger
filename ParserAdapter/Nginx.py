from ParserAdapter.BaseAdapter import Adapter
from parse import parse,search,findall
import os,json

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



    def __init__(self,*args ,**kwargs):
        super(Handler,self).__init__(*args,**kwargs)
        print('Nginx')


    def get_log_format(self):
        return [
            {'$remote_addr': {'desc': '客户端IP' , 'example':'127.0.0.1' }},
            {'$http_x_forwarded_for': {'desc': '客户端代理IP多个逗号分割' ,'example':'203.98.182.163, 203.98.182.163'}},
            {'$request': {'desc': '请求信息' ,'example':'GET /api/server/?size=50&page=1 HTTP/1.1'}},
            {'$request_body': {'desc': 'post提交的数据' ,'example':'name=xxx&age=18'}},
            {'$request_length': {'desc': '请求的字节长度' ,'example':'988'}},
            {'$request_time': {'desc': '请求花费的时间' ,'example':'0.018'}},
            {'$upstream_response_time': {'desc': 'nginx交给后端cgi响应的时间(小于$request_time)' ,'example':'0.018'}},
            {'$status': {'desc': '请求状态码' ,'example':'200'}},
            {'$bytes_sent':{'desc':'发送给客户端的总字节数(包括响应头)' ,'example':'113'} },
            {'$body_bytes_sent':{'desc':'发送给客户端的总字节数(不包括响应头)' ,'example':'266'} },
            {'$connection':{'desc':'连接到序列号' ,'example':'26'} },
            {'$connection_requests':{'desc':'每个连接到序列号的请求次数' ,'example':'3'} },
            {'$http_referer':{'desc':'请求的来源网址' ,'example':'www.baidu.com'} },
            {'$http_user_agent':{'desc':'客户端的UA信息' ,'example':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'} },
            {'$upstream_addr':{'desc':'后端接收请求的服务的ip或地址' ,'example':'unix:/tmp/php-cgi-71.sock'} },
            {'$time_iso8601':{'desc':'iso8601时间格式' ,'example':'2020-09-11T15:20:43+08:00'} },
            {'$time_local':{'desc':'本地时间格式' ,'example':'11/Sep/2020:15:20:43 +0800'} },

        ]

    def set_log_format(self,log_format ,type='string'):
        if type not in ['string','json']:
            raise ValueError('设置日志格式 type 只支持string或json格式')
        pass






