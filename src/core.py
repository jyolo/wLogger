# coding=UTF-8
from multiprocessing import Queue,Process
from redis import Redis,exceptions as redis_exceptions
from configparser import ConfigParser
from threading import Thread,RLock
from collections import deque
from pymongo import MongoClient,errors as pyerrors
from src.ip2Region import Ip2Region
import time,shutil,json,traceback,os,platform,importlib,sys,threading


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus



class Base(object):
    conf = None

    def __init__(self):
        self._root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.conf = self.__getConfig()

    def __getConfig(self):
        config_path = self._root + '/config.ini'
        if ( not os.path.exists(config_path) ):
            raise FileNotFoundError('config.ini not found in the project root path')

        conf = ConfigParser()
        conf.read(config_path, encoding="utf-8")

        return conf

    def _getRedisQueue(self):

        try:

            return Redis(
                host=self.conf['redis']['host'],
                port=int(self.conf['redis']['port']),
                password=str(self.conf['redis']['password']),
                db=self.conf['redis']['db'],
            )

        except redis_exceptions.RedisError as e:
            self.event['stop'] = e.args[0]


    def _getMongodbQueue(self):

        if self.conf['mongodb']['username'] and self.conf['mongodb']['password']:
            mongo_url = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                        (
                            quote_plus(self.conf['mongodb']['username']),
                            quote_plus(self.conf['mongodb']['password']),
                            self.conf['mongodb']['host'],
                            int(self.conf['mongodb']['port']),
                            self.conf['mongodb']['db']
                        )

        else:
            mongo_url = 'mongodb://%s:%s/?authSource=%s' % \
                        (
                            self.conf['mongodb']['host'],
                            int(self.conf['mongodb']['port']),
                            self.conf['mongodb']['db']
                        )

        mongodb = MongoClient(mongo_url)
        mongodb_client = mongodb[self.conf['mongodb']['db']]

        return mongodb_client


# 日志解析
class loggerParse(object):


    def __init__(self ,server_type,server_conf = None):

        self.server_type = server_type
        self.__handler = self.__findHandlerAdapter(server_type)()
        self.format = self.__handler.getLogFormat()
        if server_conf:
            self.logger_format = self.__handler.getLoggerFormatByServerConf(server_conf_path=server_conf)


    def getLogFormatByConfStr(self,log_format_conf,log_format_name):

        return  self.__handler.getLogFormatByConfStr( log_format_conf,log_format_name,'string')


    def parse(self,log_format_name,log_line):
        return  self.__handler.parse(log_format_name=log_format_name,log_line=log_line)



    def __findHandlerAdapter(self,server_type):
        handler_module = 'ParserAdapter.%s' % server_type.lower().capitalize()
        try:

            return importlib.import_module(handler_module).Handler
        except ModuleNotFoundError:
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/ParserAdapter')
            return importlib.import_module(handler_module).Handler




# 生产者 实时读取日志 && 切割日志 && 上报服务器状况
class Reader(Base):


    event = {
        'cut_file' : 0,
        'stop' : None,
    }

    def __init__(self,log_file_conf = None ):
        super(Reader, self).__init__()

        self.log_path = log_file_conf['file_path']
        if len(log_file_conf['log_format_name']) == 0:
            self.log_format_name = 'defualt'
        else:
            self.log_format_name = log_file_conf['log_format_name']

        self.node_id = self.conf['inputer']['node_id']

        # 最大写入队列的数据量
        if 'max_batch_push_queue_size' in self.conf['inputer']:
            self.max_batch_push_queue_size = int(self.conf['inputer']['max_batch_push_queue_size'])
        else:
            self.max_batch_push_queue_size = 5000

        # 最大重试打开文件次数
        if 'max_retry_open_file_time' in self.conf['inputer']:
            self.max_retry_open_file_time = int(self.conf['inputer']['max_retry_open_file_time'])
        else:
            self.max_retry_open_file_time = 10

        # 最大重试链接 queue的次数
        if 'max_retry_reconnect_time' in self.conf['inputer']:
            self.max_retry_reconnect_time = int(self.conf['inputer']['max_retry_reconnect_time'])
        else:
            self.max_retry_reconnect_time = 20




        self.app_name = log_file_conf['app_name']
        self.server_type = log_file_conf['server_type']
        self.read_type = log_file_conf['read_type']
        self.cut_file_type = log_file_conf['cut_file_type']
        self.cut_file_point = log_file_conf['cut_file_point']
        try:
            self.cut_file_save_dir = log_file_conf['cut_file_save_dir']
        except KeyError as e:
            self.cut_file_save_dir = None


        log_prev_path = os.path.dirname(log_file_conf['file_path'])

        if platform.system() == 'Linux':
            self.newline_char = '\n'
            import pwd
            """
                这里需要将 nginx 日志的所属 目录修改为 www 否则在切割日志的时候 kill -USR1 pid 之后 日志文件会被重新打开但是因权限问题不会继续写入文件中
            """
            # 检查日志目录所属用户 ; 不是 www 则修改成 www
            if pwd.getpwuid(os.stat(log_prev_path).st_uid).pw_name != 'www' and platform.system() == 'Linux':
                try:
                    www_uid = pwd.getpwnam('www').pw_uid
                    os.chown(log_prev_path, www_uid, www_uid)
                except PermissionError as e:
                    exit('权限不足 : 修改目录: %s 所属用户和用户组 为 www 失败 ' % (log_prev_path))



        elif platform.system() == 'Windows':
            self.newline_char = '\r\n'

        self.dqueue = deque()

        self.queue_key = self.conf['inputer']['queue_name']

        self.server_conf = loggerParse(log_file_conf['server_type'],self.conf[log_file_conf['server_type']]['server_conf']).logger_format

        
        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()


    def __getFileFd(self):
        try:
            return open(self.log_path, mode='r+' ,newline=self.newline_char)

        except FileNotFoundError as e:
            self.event['stop'] = self.log_path + ' 文件不存在'
            return False


    def __cutFileHandle(self,server_pid_path,log_path ,target_path = None ):
        start_time = time.perf_counter()
        print("\n start_time -------cutting file start ---  %s \n" % (
             start_time))

        file_suffix = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime())

        files_arr = log_path.split('/')
        log_name = files_arr.pop().replace('.log', '')
        log_path_dir = '/'.join(files_arr)


        if target_path :
            target_dir = target_path + '/' + log_name
        else:
            target_dir = log_path_dir + '/' + log_name

        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir)
            except Exception as e:
                self.event['stop'] = '日志切割存储目录创建失败'
                return

        target_file = target_dir + '/' + log_name + '_' + file_suffix

        # 这里需要注意 日志目录的 权限 是否有www  否则会导致 ngixn 重开日志问件 无法写入的问题
        cmd = 'kill -USR1 `cat %s`' % ( server_pid_path )

        try:
            shutil.move(log_path, target_file)
        except Exception as e:
            self.event['stop'] = '切割日志失败 : ' + target_file + ' ;' + e.args[1]
            return

        res = os.popen(cmd)
        if  len(res.readlines()) > 0:
            print(res.readlines())
            self.event['stop'] = 'reload 服务器进程失败'
            return


        end_time = time.perf_counter()
        print(';;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s' % (time.time()))


    def cutFile(self):

        while True:
            time.sleep(1)
            if self.event['stop']:
                print( '%s ; cutFile threading stop pid: %s' % (self.event['stop'] , os.getpid()))
                return

            try:
                server_pid_path = self.conf[self.server_type]['pid_path']
                if not os.path.exists(server_pid_path):
                    self.event['stop'] = server_pid_path + '配置项 server nginx_pid_path 不存在'
                    continue

                if not os.path.exists(self.log_path):
                    self.event['stop'] = self.log_path + ' 不存在'
                    continue

            except KeyError as e:
                self.event['stop'] = self.server_type + '配置项缺失'
                continue

            if self.cut_file_type not in ['filesize', 'time']:
                self.event['stop'] = 'cut_file_type 只支持 filesize 文件大小 或者 time 指定每天的时间'
                continue


            self.lock.acquire()

            if self.cut_file_type == 'filesize' :

                try:
                    now = time.strftime("%H:%M", time.localtime(time.time()))
                    # print('cut_file_type: filesize ;%s ---pid: %s----thread_id: %s--- %s ---------%s' % (  now, os.getpid(), threading.get_ident(), self.cut_file_point, self.cutting_file))

                    # 文件大小 单位 M
                    file_size = round(os.path.getsize(self.log_path) / (1024 * 1024))
                    if file_size < int(self.cut_file_point):
                        self.lock.release()
                        continue

                except FileNotFoundError as e:
                    self.event['stop'] = self.log_path + '文件不存在'
                    self.lock.release()
                    continue

                self.__cutFileHandle(server_pid_path , self.log_path , target_path = self.cut_file_save_dir)
                self.event['cut_file'] = 1

            elif self.cut_file_type == 'time':

                now = time.strftime("%H:%M" , time.localtime(time.time()) )
                # print('cut_file_type: time ;%s ---pid: %s----thread_id: %s--- %s ---------%s' % (
                # now,os.getpid(), threading.get_ident(), self.cut_file_point, self.cutting_file))

                if now == self.cut_file_point and self.cutting_file == False:
                    self.__cutFileHandle(server_pid_path, self.log_path ,target_path = self.cut_file_save_dir)
                    self.cutting_file = True
                    self.event['cut_file'] = 1
                elif now == self.cut_file_point and self.cutting_file == True and  self.event['cut_file'] == 1:
                    self.event['cut_file'] == 0


                elif now != self.cut_file_point:
                    self.cutting_file = False
                    self.event['cut_file'] = 0


            self.lock.release()

    def pushQueueToRedis(self):
        try:

            redis = self._getRedisQueue()
            pipe = redis.pipeline()
        except redis_exceptions.RedisError  as e:
            self.event['stop'] = 'redis 链接失败'

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            if self.event['stop']:
                print( '%s ; pushQueue threading stop pid: %s ---- tid: %s ' % (self.event['stop'] ,os.getpid() ,threading.get_ident() ))
                return

            try:

                # 重试连接queue的时候; 不再从 dqueue 中拿数据
                if retry_reconnect_time == 0:

                    start_time = time.perf_counter()
                    # print("\n pushQueue -------pid: %s -tid: %s-  started \n" % ( os.getpid(), threading.get_ident()))

                    for i in range(self.max_batch_push_queue_size):
                        try:
                            line = self.dqueue.pop()
                        except IndexError as e:
                            # print("\n pushQueue -------pid: %s -tid: %s- wait for data ;queue len: %s---- start \n" % ( os.getpid(), threading.get_ident(), len(list(self.dqueue))))
                            break

                        data = {}
                        data['node_id'] = self.node_id
                        data['app_name'] = self.app_name
                        data['log_format_name'] = self.log_format_name

                        data['line'] = line.strip()

                        try:
                            data['log_format_str'] = self.server_conf[self.log_format_name].strip()
                        except KeyError as e:
                            self.event['stop'] = self.log_format_name + '日志格式不存在'
                            break

                        data = json.dumps(data)
                        pipe.lpush(self.queue_key, data)


                res = pipe.execute()
                if len(res):
                    retry_reconnect_time = 0
                    end_time = time.perf_counter()
                    print("\n pushQueue -------pid: %s -tid: %s- push data to queue :%s ; queue_len : %s----耗时:%s \n"
                          % (os.getpid(), threading.get_ident(), len(res),redis.llen(self.queue_key), round(end_time - start_time, 2)))

                    self.event['stop'] = 1111

            except redis_exceptions.RedisError as e:


                retry_reconnect_time = retry_reconnect_time + 1

                if retry_reconnect_time >= self.max_retry_reconnect_time :
                    self.event['stop'] = 'pushQueue 重试连接 queue 超出最大次数'
                else:
                    time.sleep(2)
                    print('pushQueue -------pid: %s -tid: %s-  push data fail; reconnect Queue %s times' % (os.getpid() , threading.get_ident() , retry_reconnect_time))

                continue

    def pushQueueToMongodb(self):
        try:
            mongodb = self._getMongodbQueue()
        except pyerrors.PyMongoError  as e:
            self.event['stop'] = 'mongodb 链接失败'

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            if self.event['stop']:
                print('%s ; pushQueue threading stop pid: %s ---- tid: %s ' % (
                self.event['stop'], os.getpid(), threading.get_ident()))
                return

            try:


                # 重试连接queue的时候; 不再从 dqueue 中拿数据
                if retry_reconnect_time == 0:
                    _queuedata = []

                    start_time = time.perf_counter()
                    # print("\n pushQueue -------pid: %s -tid: %s-  started \n" % ( os.getpid(), threading.get_ident()))

                    for i in range(self.max_batch_push_queue_size):
                        try:
                            line = self.dqueue.pop()
                        except IndexError as e:
                            # print("\n pushQueue -------pid: %s -tid: %s- wait for data ;queue len: %s---- start \n" % ( os.getpid(), threading.get_ident(), len(list(self.dqueue))))
                            break

                        data = {}
                        data['node_id'] = self.node_id
                        data['app_name'] = self.app_name
                        data['log_format_name'] = self.log_format_name

                        data['line'] = line.strip()

                        try:
                            data['log_format_str'] = self.server_conf[self.log_format_name].strip()
                        except KeyError as e:
                            self.event['stop'] = self.log_format_name + '日志格式不存在'
                            break

                        data['out_queue'] = 0
                        data['add_time'] = time.time()

                        _queuedata.append(data)

                        # data['out_queue'] = 0
                        # _queuedata.append(data)


                if len(_queuedata):

                    # mongodb[self.queue_key].create_index([("out_queue",1)] ,background = True)

                    # res = mongodb[self.queue_key].insert_many(_queuedata ,ordered=False,bypass_document_validation=True)
                    res = mongodb[self.queue_key].insert_many(_queuedata ,ordered=False)

                    # total = mongodb[self.queue_key].count_documents({'out_queue':0})

                    end_time = time.perf_counter()
                    print(
                        "\n pushQueue -------pid: %s -tid: %s- push data to queue :%s ; queue_len : %s----耗时:%s \n"
                        % (os.getpid(), threading.get_ident(),  len(res.inserted_ids), 0,
                           round(end_time - start_time, 2)))




            except pyerrors.PyMongoError as e:

                retry_reconnect_time = retry_reconnect_time + 1

                if retry_reconnect_time >= self.max_retry_reconnect_time:
                    self.event['stop'] = 'pushQueue 重试连接 queue 超出最大次数'
                else:
                    time.sleep(2)
                    print('pushQueue -------pid: %s -tid: %s-  push data fail: %s ; reconnect Queue %s times' % (
                    os.getpid(),e.args, threading.get_ident(), retry_reconnect_time))

                continue

    def readLog(self):

        position = 0

        if self.read_type not in ['head','tail']:
            self.event['stop'] = 'read_type 只支持 head 从头开始 或者 tail 从末尾开始'

        try:
            if self.read_type == 'head':
                self.fd.seek(position, 0)
            elif self.read_type == 'tail':
                self.fd.seek(position, 2)
        except Exception as e:
            self.event['stop'] = self.log_path + ' 文件句柄 seek 错误'


        max_retry_open_file_time = 3
        retry_open_file_time = 0
        while True:
            time.sleep(0.5)

            if self.event['stop']:
                print( '%s ; read threading stop pid: %s' % (self.event['stop'] ,os.getpid()))
                return

            start_time = time.perf_counter()
            # print("\n start_time -------pid: %s -- read file---queue len: %s---- %s \n" % ( os.getpid(), len(list(self.dqueue)), round(start_time, 2)))


            for line in self.fd:
                # 不是完整的一行继续read
                if line.find(self.newline_char) == -1:
                    continue

                self.dqueue.append(line)


            end_time = time.perf_counter()
            print("\n end_time -------pid: %s -- read file---line len :%s --- 耗时:%s \n" % (os.getpid(), len(list(self.dqueue)), round(end_time - start_time, 2)))

            if self.event['cut_file'] == 1 and self.event['stop'] == None:
                # 防止 重启进程服务后 新的日志文件并没有那么快重新打开
                time.sleep(1.5)
                print('--------------------reopen file--------------------at: %s' % time.time())

                self.fd.close()
                self.fd = self.__getFileFd()
                try:
                    self.fd.seek(0)
                except AttributeError as e:
                    time.sleep(1)
                    retry_open_file_time = retry_open_file_time + 1
                    if retry_open_file_time >= max_retry_open_file_time:
                        self.event['stop'] = '重新打开文件超过最大次数 %s ' % max_retry_open_file_time
                    continue

                self.event['cut_file'] = 0



    def runMethod(self,method_name):
        print('pid:%s , %s ,%s' % (os.getpid() ,method_name  ,time.perf_counter()))
        getattr(self,method_name)()



# 消费者 解析日志 && 存储日志
class OutputCustomer(Base):

    def __init__(self ):

        super(OutputCustomer,self).__init__()

        self.inputer_queue_type = self.conf['outputer']['queue']
        self.inputer_queue = self._getQueue()
        self.inputer_queue_key = self.conf['outputer']['queue_name']

        print('123')
        exit()

        self.save_engine_conf = dict( self.conf[ self.conf['outputer']['save_engine'] ])
        self.save_engine_name = self.conf['outputer']['save_engine'].lower().capitalize()


        self.call_engine = 'saveTo%s' %  self.save_engine_name
        self.server_type = self.conf['outputer']['log_server_type']

        self.logParse = loggerParse(self.conf['outputer']['log_server_type'] ,server_conf=None)

        ip_data_path = os.path.dirname(__file__) + '/ip2region.db'
        if not os.path.exists(ip_data_path):
            raise FileNotFoundError('ip2region.db 数据库不存在')

        self.ip_parser = Ip2Region(ip_data_path)

        if 'max_batch_insert_db_size' in self.conf['outputer']:
            self.max_batch_insert_db_size = int(self.conf['outputer']['max_batch_insert_db_size'])
        else:
            self.max_batch_insert_db_size = 500

        if not hasattr(self , self.call_engine ):
            raise ValueError('Outputer 未定义 "%s" 该存储方法' %  self.save_engine_name)



    def getQueueData(self):


        start_time = time.perf_counter()
        # print("\n outputerer -------pid: %s -- take from queue len: %s---- start \n" % (
        #     os.getpid(), self.inputer_queue.llen(self.inputer_queue_key)))

        pipe = self.inputer_queue.pipeline()

        queue_len = self.inputer_queue.llen(self.inputer_queue_key)
        if queue_len >= self.max_batch_insert_db_size:
            num = self.max_batch_insert_db_size
        else:
            num = queue_len


        for i in range(num):
            res = pipe.lpop(self.inputer_queue_key)



        queue_list = pipe.execute()

        # 过滤掉None
        if queue_list.count(None) :
            queue_list = list(filter(None,queue_list))


        end_time = time.perf_counter()
        if len(queue_list):
            print("\n outputerer -------pid: %s -- take len: %s ; queue len : %s----end 耗时: %s \n" % ( os.getpid(), len(queue_list),self.inputer_queue.llen(self.inputer_queue_key), round(end_time - start_time, 2)))


        return queue_list


    def __parse_time_str(self,data):
        if self.server_type == 'nginx':
            if 'time_iso8601' in data:
                _strarr = data['time_iso8601'].split('+')
                ts = time.strptime(_strarr[0],'%Y-%m-%dT%H:%M:%S')
                del data['time_iso8601']

            if 'time_local' in data:
                _strarr = data['time_local'].split('+')
                ts = time.strptime(_strarr[0].strip() ,'%d/%b/%Y:%H:%M:%S')
                del data['time_local']

            data['time_str'] = time.strftime('%Y-%m-%d %H:%M:%S',ts)
            data['timestamp'] = int(time.mktime(ts))




        if self.server_type == 'apache':
            pass




        return data

    def __parse_request_url(self,data):
        if self.server_type == 'nginx':
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

                    data['server_protocol'] = _strarr[2]

                    del data['request']

                if 'request_uri' in data:
                    _strarr = data['request_uri'].split('?')

                    data['request_url'] = _url[0]
                    data['args'] = _url[1]

                    del data['request_uri']
            except IndexError as e:
                print(data)
                print(_strarr)
                exit()



        if self.server_type == 'apache':
            pass

        return data

    def __parse_ip_to_area(self,data):

        if self.server_type == 'nginx':
            if 'remote_addr' in data:


                try:
                    res = self.ip_parser.memorySearch(data['remote_addr'])
                    _arg = res['region'].decode('utf-8').split('|')

                    # _城市Id|国家|区域|省份|城市|ISP_
                    data['isp'] = _arg[-1]
                    data['city'] = _arg[-2]
                    data['city_id'] = int(res['city_id'])
                    data['province'] = _arg[-3]
                    data['country'] = _arg[0]
                except Exception as e:
                    data['isp'] = -1
                    data['city'] = -1
                    data['city_id'] = -1
                    data['province'] = -1
                    data['country'] = -1

        if self.server_type == 'apache':
            pass

        return data



    def parse_line_data(self,line):

        line_data = json.loads(line)

        try:

            # 预编译对应的正则
            self.logParse.getLogFormatByConfStr(line_data['log_format_str'], line_data['log_format_name'])

            line_data['line'] = line_data['line'].strip()

            # parse_data = self.logParse.parse(parse_data, line_data['line'])
            parse_data = self.logParse.parse(line_data['log_format_name'], line_data['line'])

        except Exception as e:
            traceback.print_exc()
            exit()

        # 解析时间
        parse_data = self.__parse_time_str(parse_data)
        # 解析url
        parse_data = self.__parse_request_url(parse_data)
        # 解析IP 成地域
        parse_data = self.__parse_ip_to_area(parse_data)

        del line_data['log_format_name']
        del line_data['log_format_str']
        del line_data['line']

        line_data.update(parse_data)


        return line_data


    def saveToMongodb(self):


        if self.save_engine_conf['username'] and self.save_engine_conf['password']:
            mongo_url = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                (
                    quote_plus(self.save_engine_conf['username']),
                    quote_plus(self.save_engine_conf['password']),
                    self.save_engine_conf['host'],
                    int(self.save_engine_conf['port']),
                    self.save_engine_conf['db']
                )
        else:
            mongo_url = 'mongodb://%s:%s/?authSource=%s' % \
                        (
                            self.save_engine_conf['host'],
                            int(self.save_engine_conf['port']),
                            self.save_engine_conf['db']
                        )


        mongodb = MongoClient( mongo_url)
        mongodb_client = mongodb[ self.save_engine_conf['db'] ][ self.save_engine_conf['collection'] ]

        if 'max_retry_reconnect_time' in  self.conf['outputer']:
            max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
        else:
            max_retry_reconnect_time = 3

        retry_reconnect_time = 0

        while True:
            time.sleep(0.1)
            # 重试链接的时候 不再 从队列中取数据
            if retry_reconnect_time == 0:
                num = self.inputer_queue.llen(self.inputer_queue_key)

                if num == 0:
                    # print('pid: %s wait for data' % os.getpid())
                    continue

                queue_list = self.getQueueData()


                if len(queue_list) == 0:
                    print('pid: %s wait for data' % os.getpid())
                    continue

                start_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- reg data len: %s---- start \n" % (
                    os.getpid(), len(queue_list)))

                backup_for_push_back_queue = []
                insertList = []
                for i in queue_list:
                    if not i:
                        continue

                    line = i.decode(encoding='utf-8')
                    backup_for_push_back_queue.append(line)

                    line_data = self.parse_line_data(line)
                    insertList.append(line_data)

                end_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- reg datas len: %s---- end 耗时: %s \n" % (
                    os.getpid(), len(insertList), round(end_time - start_time, 2)))


            if len(insertList):
                try:
                    start_time = time.perf_counter()
                    print("\n outputerer -------pid: %s -- insert into mongodb: %s---- start \n" % (os.getpid(), len(insertList)))

                    res = mongodb_client.insert_many(insertList, ordered=False)


                    retry_reconnect_time = 0

                    end_time = time.perf_counter()
                    print("\n outputerer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (os.getpid(), len(res.inserted_ids), round(end_time - start_time, 2)))

                except pyerrors.PyMongoError as e:
                    time.sleep(1)
                    retry_reconnect_time = retry_reconnect_time + 1
                    if retry_reconnect_time >= max_retry_reconnect_time:
                        self.push_back_to_queue(backup_for_push_back_queue)
                        exit('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                    else:
                        print("\n outputerer -------pid: %s -- retry_reconnect_mongodb at: %s time---- \n" % (os.getpid() ,retry_reconnect_time) )
                        continue

    def saveToMysql(self):
        pass

    #退回队列
    def push_back_to_queue(self,data_list):
        for item in data_list:
            self.inputer_queue.lpush(self.inputer_queue_key , item)





if __name__ == "__main__":
    pass

