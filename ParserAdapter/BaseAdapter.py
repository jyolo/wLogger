from abc import abstractmethod,ABCMeta
from Src.ip2Region import Ip2Region
import os,time

# 解析错误
class ParseError(Exception):pass
# 预编译错误
class ReCompile(Exception):pass



class Adapter():
    __metaclass__ = ABCMeta

    LOG_FORMAT_SPLIT_TAG = '<@>'

    ip_parser = None
    log_line_pattern_dict = {}

    def __init__(self,*args,**kwargs):

        if self.ip_parser == None:
            ip_data_path = os.path.dirname(os.path.dirname(__file__)) + '/Src/ip2region.db'

            if not os.path.exists(ip_data_path):
                raise FileNotFoundError('ip2region.db 数据库不存在')

            self.ip_parser = Ip2Region(ip_data_path)

    @abstractmethod
    def getLogFormat(self): pass

    @abstractmethod
    def parse(self): pass

    @abstractmethod
    def getLogFormatByConfStr(self): pass

    def getLoggerFormatByServerConf(self):pass

    @abstractmethod
    def rotatelog(self):pass

    # 解析 ip 新增地域字段  isp city city_id province country
    @abstractmethod
    def parse_ip_to_area(self,ip):
        data = {}
        try:
            res = self.ip_parser.memorySearch(ip)

            _arg = res['region'].decode('utf-8').split('|')

            # _城市Id|国家|区域|省份|城市|ISP_
            data['isp'] = _arg[-1]
            data['city'] = _arg[-2]
            data['city_id'] = int(res['city_id'])
            data['province'] = _arg[-3]
            data['country'] = _arg[0]

        except Exception as e:
            data['isp'] = 0
            data['city'] = 0
            data['city_id'] = 0
            data['province'] = 0
            data['country'] = 0

        return data

    # 解析requset字段 变成 request_method ,request_url ,args ,server_protocol
    @abstractmethod
    def parse_request_to_extend(self,request_data):
        data = {}

        try:


            if len(request_data.strip()) == 0:
                data['request_method'] = ''
                data['request_url'] = ''
                data['args'] = ''
                data['server_protocol'] = ''
                return data

            _strarr = request_data.split(' ')

            if(len(_strarr) == 1) :
                data['request_method'] = ''
                data['request_url'] = _strarr[0]
                data['args'] = ''
                data['server_protocol'] = ''
                return data



            data['request_method'] = _strarr[0]
            _url = _strarr[1].split('?')

            if len(_url) > 1:
                data['request_url'] = _url[0]
                data['args'] = _url[1]
            else:
                data['request_url'] = _url[0]
                data['args'] = ''


            data['server_protocol'] = _strarr[2]


            if 'request_uri' in data:
                _strarr = data['request_uri'].split('?')

                data['request_url'] = _url[0]
                data['args'] = _url[1]


            return data
        except IndexError as e:
            # 异常攻击流量数据 解析错误 不在抛出exception 直接原样返回数据 以供分析
            data['request_method'] = ''
            data['request_url'] = request_data
            data['args'] = ''
            data['server_protocol'] = ''

            return data


    # 解析 time_iso8601 time_local 变成 time_str timestamp
    @abstractmethod
    def parse_time_to_str(self,time_type , time_data):

        data = {}
        time_data = time_data.replace('[', '').replace(']', '')

        if 'time_iso8601' == time_type:
            _strarr = time_data.split('+')
            ts = time.strptime(_strarr[0], '%Y-%m-%dT%H:%M:%S')



        if 'time_local' == time_type:
            _strarr = time_data.split('+')
            ts = time.strptime(_strarr[0].strip(), '%d/%b/%Y:%H:%M:%S')


        data['time_str'] = time.strftime('%Y-%m-%d %H:%M:%S', ts)
        data['timestamp'] = int(time.mktime(ts))




        return data