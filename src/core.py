# coding=UTF-8
from multiprocessing import Queue,Process
from configparser import ConfigParser
from threading import Thread,RLock
from collections import deque
from src.ip2Region import Ip2Region
import time,shutil,json,traceback,os,platform,importlib,sys,threading


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

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
        except ImportError:
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/ParserAdapter')
            return importlib.import_module(handler_module).Handler


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

    def _findAdapterHandler(self,adapter_type,queue_type):

        if adapter_type not in ['queue','storage']:
            raise ValueError('%s Adapter 类型不存在' % adapter_type)

        handler_module = '%sAdapter.%s' % (adapter_type.lower().capitalize(),queue_type.lower().capitalize())

        try:
            if adapter_type == 'queue':
                return importlib.import_module(handler_module).QueueAp
            if adapter_type == 'storage':
                return importlib.import_module(handler_module).StorageAp

        except ImportError:
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

        # 内部队列
        self.dqueue = deque()

        self.queue_key = self.conf['inputer']['queue_name']

        self.server_conf = loggerParse(log_file_conf['server_type'],self.conf[log_file_conf['server_type']]['server_conf']).logger_format

        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()

        # 外部队列handle
        self.queue_handle = self._findAdapterHandler('queue',self.conf['inputer']['queue']).initQueue(self)


    def __getFileFd(self):
        try:
            return open(self.log_path, mode='r+' ,newline=self.newline_char)

        except OSError as e:
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


    def pushDataToQueue(self):

        self.queue_handle.pushDatatoQueue()


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
        self.inputer_queue_key = self.conf['outputer']['queue_name']


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


        # 外部队列handle
        self.queue_handle = self._findAdapterHandler('queue',self.conf['outputer']['queue']).initQueue(self)
        # 外部 存储引擎
        self.storage_handle = self._findAdapterHandler('storage',self.conf['outputer']['save_engine']).initStorage(self)



    # def getQueueDataFromMongodb(self):
    #     _data = []
    #     # print('pid: %s ;queuesize : ' % os.getpid(),self.multi_queue.qsize() )
    #     start_time = time.perf_counter()
    #
    #     for i in range( int(self.conf['outputer']['max_batch_insert_db_size']) ):
    #
    #         res = self.inputer_queue[self.inputer_queue_key].find_and_modify(
    #             query={'out_queue':0},
    #             update = {'$set': {'out_queue': 1}},
    #             remove = False,
    #             sort =  {'add_time': -1},
    #         )
    #         if not res:
    #             break
    #
    #         _data.append(res)
    #
    #
    #     # print('pid: %s ;take data from queue %s ;total queue size : %s ' % (os.getpid() , len(_data) ,self.multi_queue.qsize() ))
    #     # print(self.multi_queue.qsize())
    #     end_time = time.perf_counter()
    #     print('pid:%s ; 耗时 %s' % (os.getpid() ,end_time - start_time))
    #     return  _data
    #
    #     # self.inputer_queue = self._getMongodbQueue()
    #     #
    #     # res = self.inputer_queue[self.inputer_queue_key]\
    #     #     .find({'out_queue':0})\
    #     #     .sort([('add_time',-1)])\
    #     #     .limit(self.max_batch_insert_db_size)
    #     #
    #     #
    #     #
    #     # page_collection = self.inputer_queue_key+'_take_page'
    #     #
    #     # p = self.inputer_queue[page_collection].find_and_modify(
    #     #     query= { },
    #     #     update={'$set':{'pid':os.getpid() ,'page': 10 }}
    #     # )
    #     # print(p)
    #     # exit()
    #     #
    #     # return list(res)
    #
    #
    # def getQueueDataFromRedis(self):
    #
    #     start_time = time.perf_counter()
    #     # print("\n outputerer -------pid: %s -- take from queue len: %s---- start \n" % (
    #     #     os.getpid(), self.inputer_queue.llen(self.inputer_queue_key)))
    #     self.inputer_queue = self._getRedisQueue()
    #
    #     pipe = self.inputer_queue.pipeline()
    #
    #     queue_len = self.inputer_queue.llen(self.inputer_queue_key)
    #     if queue_len >= self.max_batch_insert_db_size:
    #         num = self.max_batch_insert_db_size
    #     else:
    #         num = queue_len
    #
    #     for i in range(num):
    #         pipe.lpop(self.inputer_queue_key)
    #
    #     queue_list = pipe.execute()
    #
    #     # 过滤掉None
    #     if queue_list.count(None) :
    #         queue_list = list(filter(None,queue_list))
    #
    #
    #     end_time = time.perf_counter()
    #     if len(queue_list):
    #         print("\n outputerer -------pid: %s -- take len: %s ; queue len : %s----end 耗时: %s \n" % ( os.getpid(), len(queue_list),self.inputer_queue.llen(self.inputer_queue_key), round(end_time - start_time, 2)))
    #
    #
    #     return queue_list


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

    def _get_queue_count_num(self):

        if self.inputer_queue_type == 'mongodb':
            return self.inputer_queue[self.inputer_queue_key].count()
        elif self.inputer_queue_type == 'redis':
            return self.inputer_queue.llen(self.inputer_queue_type)


    def parse_line_data(self,line):

        if isinstance(line ,str):
            line_data = json.loads(line)
        else:
            line_data = line

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


    def getQueueData(self):
        return self.queue_handle.getDataFromQueue()

    def saveToStorage(self ):

        self.storage_handle.pushDatatoStorage()



    def saveToMysql(self):
        pass

    #退回队列
    def push_back_to_queue(self,data_list):
        if self.inputer_queue_type == 'redis':
            for item in data_list:
                self.inputer_queue.lpush(self.inputer_queue_key , item)

        pass




if __name__ == "__main__":
    pass

