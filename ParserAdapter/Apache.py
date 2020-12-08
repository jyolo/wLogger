from ParserAdapter.BaseAdapter import Adapter,ParseError,ReCompile
import re,os,shutil,json


class Handler(Adapter):

    def __init__(self,*args ,**kwargs):
        super(Handler,self).__init__(*args,**kwargs)


    def getLogFormat(self):
        # "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\" %I %O"
        return {
            ### 非 nginx 日志变量 附加自定义字段
            '$node_id': {
                'mysql_field_type': 'varchar(255)',
                'mysql_key_field': True
            },
            '$app_name': {
                'mysql_field_type': 'varchar(255)',
            },
            ### 非 nginx 日志变量 附加自定义字段

            '%h':{
                'nickname': 'ip',
                'mysql_field_type': 'varchar(15)',
                'mysql_key_field': [
                    '%t.timestamp',
                    ['%>s', '%r.request_url', '%r.request_method']
                ],
                'extend_field': {
                    'isp': {
                        'mysql_field_type': 'varchar(30)',
                    },
                    'city': {
                        'mysql_field_type': 'varchar(30)',
                        'mysql_key_field': ['%>s'],
                    },
                    'city_id': {
                        'mysql_field_type': 'int(10)',
                    },
                    'province': {
                        'mysql_field_type': 'varchar(30)',
                        'mysql_key_field': [
                            '%t.timestamp',
                            '%t.timestamp',
                        ],
                    },
                    'country': {
                        'mysql_field_type': 'varchar(30)',
                    }
                }
            },
            '%l':{
                'nickname': 'remote_logname',
            },
            '%u':{
                'nickname': 'remote_user'
            },
            '%t': {
                'nickname': 'time',
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
            },
            '%r': {
                'nickname': 'request',
                'extend_field': {
                    'request_method': {
                        'mysql_field_type': 'varchar(10)',
                        'mysql_key_field': True,
                    },
                    'request_url': {
                        'mysql_field_type': 'varchar(255)',
                        'mysql_key_field': ['%t.timestamp', '%t.timestamp'],
                    },
                    'args': {
                        'mysql_field_type': 'text',
                    },
                    'server_protocol': {
                        'mysql_field_type': 'varchar(10)',
                    },
                }
            },
            '%>s': {
                'nickname': 'status_code',
            },
            '%b': {
                'nickname': 'bytes_sent',
            },
            '%D':{
                'nickname': 'request_time',
            },
            '%m':{
                'nickname': 'request_method',
            },
            '%{Referer}i': {
                'nickname': 'referer',
            },
            '%{User-Agent}i': {
                'nickname': 'ua',
            },
            '%I': {
                'nickname': 'bytes_received',
                're': '\d+',
            },
            '%O': {
                'nickname': 'bytes_sent_with_header',
                're': '\d+',
            },
        }


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

            for i in range(len(list(log_format_list))):
                key_name = log_format_list[i]

                # request 参数不支持别名
                if 'nickname' in self.getLogFormat()[ log_format_list[i]] :
                    key_name = self.getLogFormat()[ log_format_list[i]]['nickname']

                # 解析 %h 成对应的 地理位置信息 isp,city,city_id,province,country,ip,
                if key_name == '%h':
                    ip_data = self.parse_ip_to_area(matched[i])
                    data.update(ip_data)

                # 解析 %r 成 request_method ,request_url ,args ,server_protocol ,
                if key_name == '%r':
                    request_extend_data = self.parse_request_to_extend(matched[i])
                    data.update(request_extend_data)
                    del_key_name.append(key_name)

                # 解析 %t 成 timestr , timestamp
                if key_name == '%t':
                    time_data = self.parse_time_to_str('time_local',matched[i])
                    data.update(time_data)
                    del_key_name.append(key_name)

                data[key_name] = matched[i]




        # 剔除掉 解析出拓展字符串的 字段
        for i in del_key_name:
            del data[i]


        return data



    # 从配置获取所有的日志配置项
    def getLoggerFormatByServerConf(self,server_conf_path):

        # 根据配置文件 自动获取 log_format 字符串
        with open(server_conf_path, 'rb') as fd:
            content = fd.read().decode(encoding="utf-8")

        format_list = {}

        log_list = re.findall(r'LogFormat\s?\"([\S\s]*?)\"\s?(\w+)\n' ,content)


        if len(log_list) == 0:
            return format_list

        # 从日志格式字符串中提取日志变量
        for i in log_list:
            res = re.findall(r'(\%[\>|\{]?[a-zA-Z|\-|\_]+[\}|\^]?\w?)', i[0].strip())
            if len(res):
                format_list[i[1]] = {
                    'log_format_str':i[0].strip(),
                    'log_format_vars':self.LOG_FORMAT_SPLIT_TAG.join(res)
                }


        del content


        return format_list



    # 重启日志进程
    def rotatelog(self,server_conf,log_path ,target_file ):


        try:

            if not os.path.exists(server_conf['apachectl_bin']):
                raise FileNotFoundError('apachectl_bin : %s 命令不存在' % server_conf['apachectl_bin'])

            if not os.path.exists(log_path):
                raise FileNotFoundError('日志:  %s 不存在' % log_path)



            # 这里需要注意 日志目录的 权限 是否有www  否则会导致 ngixn 重开日志问件 无法写入的问题
            cmd = '%s graceful' % server_conf['apachectl_bin']
            shutil.move(log_path, target_file)

            res = os.popen(cmd)
            if  len(res.readlines()) > 0:
                cmd_res = ''
                for i in res.readlines():
                    cmd_res += i + '\n'
                raise Exception ('reload 服务器进程失败: %s' % cmd_res)

            return True

        except Exception as e:
            return '切割日志失败 : %s ; error class : %s error info : %s' % (target_file ,e.__class__, e.args)

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

    """
        根据录入的格式化字符串 返回 parse 所需 log_format 配置
    """
    def getLogFormatByConfStr(self ,log_format_str,log_format_vars ,log_format_name ,log_type):


        # 日志格式不存在 则 预编译
        if log_format_name not in self.log_line_pattern_dict:

            # 过滤下 正则中 特殊字符
            log_format_str = log_format_str.strip() \
                .replace('[', '\[').replace(']', '\]') \
                .replace('(', '\(').replace(')', '\)')


            if (log_type == 'string'):


                # 获取日志变量
                log_format_list = log_format_vars.split(self.LOG_FORMAT_SPLIT_TAG)

                # 按照日志格式顺序 替换日志变量成正则 进行预编译
                format = re.sub(r'(\%[\>|\{]?[a-zA-Z|\-|\_]+[\}|\^]?\w?)', self.__replaceLogVars, log_format_str).strip()
                try:
                    re_compile = re.compile(format, re.I)
                except re.error:
                    raise Exception('预编译错误,请检查日志字符串中是否包含特殊字符串; 日志:%s' % log_format_str)


                self.log_line_pattern_dict[log_format_name] = {
                    'log_format_list': log_format_list,
                    'log_format_recompile':re_compile
                }




