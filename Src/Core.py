# coding=UTF-8
from ParserAdapter.BaseAdapter import ParseError,ReCompile
from configparser import ConfigParser
from threading import RLock
from collections import deque
import time,json,os,platform,importlib,logging


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
        self.__handler = Base.findAdapterHandler('server',server_type)()
        self.format = self.__handler.getLogFormat()
        if server_conf:
            self.logger_format = self.__handler.getLoggerFormatByServerConf(server_conf_path=server_conf)


    def getLogFormatByConfStr(self,log_format_str,log_format_vars,log_format_name):

        return  self.__handler.getLogFormatByConfStr( log_format_str,log_format_vars,log_format_name,'string')


    def parse(self,log_format_name,log_line):
        return  self.__handler.parse(log_format_name=log_format_name,log_line=log_line)



class Base(object):
    conf = None
    CONFIG_FIEL_SUFFIX = '.ini'
    config_name = 'config'

    def __init__(self,config_name = None):
        self._root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.conf = self.__getConfig(config_name)
        self.config_name = config_name
        self.__initLogging()


    def __initLogging(self):
        LOG_FORMAT = "%(asctime)s %(levelname)s %(pathname)s %(lineno)s %(message)s "
        DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '
        if self.__class__.__name__ == 'Base':return

        logg_setting_map = {
            'Reader': 'inputer',
            'OutputCustomer': 'outputer',
        }

        conf_name = logg_setting_map[self.__class__.__name__]

        if 'log_debug' in self.conf[conf_name] and self.conf[conf_name]['log_debug'] == 'True':
            _level = logging.DEBUG
        else:
            _level = logging.INFO

        logging.basicConfig(level=_level, format=LOG_FORMAT, datefmt=DATE_FORMAT,
                            filename=r"./%s_%s.log" % (conf_name ,self.config_name) )

        self.logging = logging

    def __getConfig(self,config_name):
        if config_name:
            self.config_name = config_name

        if self.config_name.find(self.CONFIG_FIEL_SUFFIX) == -1:
            config_path = self._root + '/' + self.config_name + self.CONFIG_FIEL_SUFFIX
        else:
            config_path = self._root + '/' + self.config_name

        if ( not os.path.exists(config_path) ):
            raise FileNotFoundError('config file: %s not found ' % (config_path) )

        conf = ConfigParser()
        conf.read(config_path, encoding="utf-8")

        return conf

    @classmethod
    def findAdapterHandler(cls,adapter_type,name):

        if adapter_type not in ['server', 'queue', 'storage','traffic_analysis']:
            raise ValueError('%s Adapter 类型不存在' % adapter_type)

        if adapter_type in ['queue', 'storage']:
            handler_module = '%sAdapter.%s' % (adapter_type.lower().capitalize(), name.lower().capitalize())
            print(handler_module)
        elif adapter_type == 'traffic_analysis':
            handler_module = '%sAdapter.%s' % (adapter_type.lower().capitalize(), name.lower().capitalize())
            print(handler_module)
            exit()
        else:
            handler_module = 'ParserAdapter.%s' % name.lower().capitalize()

        if adapter_type == 'queue':
            return importlib.import_module(handler_module).QueueAp
        if adapter_type == 'storage':
            return importlib.import_module(handler_module).StorageAp
        if adapter_type == 'traffic_analysis':
            return importlib.import_module(handler_module).TrafficAnalysisAp
        if adapter_type == 'server':
            return importlib.import_module(handler_module).Handler


    def runMethod(self,method_name):
        self.logging.debug('pid:%s , %s ,%s' % (os.getpid() ,method_name  ,time.perf_counter()))
        getattr(self,method_name)()


# 生产者 实时读取日志 && 切割日志 && 上报服务器状况
class Reader(Base):

    event = {
        'cut_file' : 0,
        'stop' : None,
    }

    def __init__(self,log_file_conf = None ,config_name = None):
        super(Reader, self).__init__(config_name=config_name)

        self.log_path = log_file_conf['file_path']


        if 'log_format_name' not in log_file_conf or len(log_file_conf['log_format_name']) == 0:
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
        self.queue_handle = self.findAdapterHandler('queue',self.conf['inputer']['queue']).initQueue(self)
        self.server_handle = self.findAdapterHandler('server',log_file_conf['server_type'])()


    def __getFileFd(self):
        try:
            return open(self.log_path, mode='r+' ,newline=self.newline_char)

        except PermissionError as e:
            self.event['stop'] = self.log_path + '没有权限读取文件 请尝试sudo'
            return False

        except OSError as e:
            self.event['stop'] = self.log_path + ' 文件不存在'
            return False


    def __cutFileHandle(self,server_conf,log_path ,target_path = None ):

        start_time = time.perf_counter()
        self.logging.debug("\n start_time -------cutting file start ---  %s \n" % (
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

        res = self.server_handle.rotatelog(server_conf,log_path,target_file)
        if(isinstance(res,str) and res != True):
            self.event['stop'] = res

        end_time = time.perf_counter()
        self.logging.debug('finnish file_cut  take time : %s' % round((start_time - end_time)))


    def cutFile(self):

        while True:
            time.sleep(1)
            if self.event['stop']:
                self.logging.debug( '%s ; cutFile threading stop pid: %s' % (self.event['stop'] , os.getpid()))
                return


            if self.cut_file_type not in ['filesize', 'time']:
                self.event['stop'] = 'cut_file_type 只支持 filesize 文件大小 或者 time 指定每天的时间'
                continue


            self.lock.acquire()

            if self.cut_file_type == 'filesize' :

                try:
                    now = time.strftime("%H:%M", time.localtime(time.time()))
                    # self.logging.debug('cut_file_type: filesize ;%s ---pid: %s----thread_id: %s--- %s ---------%s' % (  now, os.getpid(), threading.get_ident(), self.cut_file_point, self.cutting_file))

                    # 文件大小 单位 M
                    file_size = round(os.path.getsize(self.log_path) / (1024 * 1024))
                    if file_size < int(self.cut_file_point):
                        self.lock.release()
                        continue

                except FileNotFoundError as e:
                    self.event['stop'] = self.log_path + '文件不存在'
                    self.lock.release()
                    continue

                self.__cutFileHandle(
                    server_conf = dict(self.conf[self.server_type]) ,
                    log_path= self.log_path ,
                    target_path = self.cut_file_save_dir
                )

                self.event['cut_file'] = 1

            elif self.cut_file_type == 'time':

                now = time.strftime("%H:%M" , time.localtime(time.time()) )
                # self.logging.debug('cut_file_type: time ;%s ---pid: %s----thread_id: %s--- %s ---------%s' % (
                # now,os.getpid(), threading.get_ident(), self.cut_file_point, self.cutting_file))

                if now == self.cut_file_point and self.cutting_file == False:
                    self.__cutFileHandle(
                        server_conf = dict(self.conf[self.server_type]) ,
                        log_path = self.log_path ,
                        target_path = self.cut_file_save_dir
                    )

                    self.cutting_file = True
                    self.event['cut_file'] = 1
                elif now == self.cut_file_point and self.cutting_file == True and  self.event['cut_file'] == 1:
                    self.event['cut_file'] == 0


                elif now != self.cut_file_point:
                    self.cutting_file = False
                    self.event['cut_file'] = 0


            self.lock.release()


    def pushDataToQueue(self):

        self.queue_handle.pushDataToQueue()


    def readLog(self):


        position = 0

        if self.read_type not in ['head','tail']:
            self.event['stop'] = 'read_type 只支持 head 从头开始 或者 tail 从末尾开始'

        if self.fd == False:
            return

        try:
            if self.read_type == 'head':
                self.fd.seek(position, 0)
            elif self.read_type == 'tail':
                self.fd.seek(position, 2)
        except Exception as e:
            self.event['stop'] = self.log_path + ' 文件句柄 seek 错误 %s ; %s' % (e.__class__ , e.args)


        max_retry_open_file_time = 3
        retry_open_file_time = 0
        while True:
            time.sleep(1)

            if self.event['stop']:
                self.logging.debug( '%s ; read threading stop pid: %s' % (self.event['stop'] ,os.getpid()))
                return

            start_time = time.perf_counter()
            # self.logging.debug("\n start_time -------pid: %s -- read file---queue len: %s---- %s \n" % ( os.getpid(), len(list(self.dqueue)), round(start_time, 2)))


            for line in self.fd:
                # 不是完整的一行继续read
                if line.find(self.newline_char) == -1:
                    continue

                self.dqueue.append(line)


            end_time = time.perf_counter()
            self.logging.debug("\n end_time -------pid: %s -- read file- %s--line len :%s --- 耗时:%s \n" % (os.getpid(),self.log_path, len(list(self.dqueue)), round(end_time - start_time, 2)))

            if self.event['cut_file'] == 1 and self.event['stop'] == None:
                # 防止 重启进程服务后 新的日志文件并没有那么快重新打开
                time.sleep(1.5)
                self.logging.debug('--------------------reopen file--------------------at: %s' % time.time())

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



# 消费者 解析日志 && 存储日志
class OutputCustomer(Base):

    def __init__(self , config_name = None):

        super(OutputCustomer,self).__init__(config_name)

        self.dqueue = deque()

        self.inputer_queue_type = self.conf['outputer']['queue']
        self.queue_key = self.conf['outputer']['queue_name']


        self.save_engine_conf = dict( self.conf[ self.conf['outputer']['save_engine'] ])
        self.save_engine_name = self.conf['outputer']['save_engine'].lower().capitalize()


        self.server_type = self.conf['outputer']['server_type']
        self.logParse = loggerParse(self.conf['outputer']['server_type'] ,server_conf=None)

        if 'max_batch_insert_db_size' in self.conf['outputer']:
            self.max_batch_insert_db_size = int(self.conf['outputer']['max_batch_insert_db_size'])
        else:
            self.max_batch_insert_db_size = 500


        # 外部队列handle
        self.queue_handle = self.findAdapterHandler('queue',self.conf['outputer']['queue']).initQueue(self)
        # 外部 存储引擎
        self.storage_handle = self.findAdapterHandler('storage',self.conf['outputer']['save_engine']).initStorage(self)

        # self.traffic_analysis_handel = self.findAdapterHandler('traffic_analysis',self.conf['outputer']['save_engine']).initStorage(self)



    def _parse_line_data(self,line):

        if isinstance(line ,str):
            line_data = json.loads(line)
        else:
            line_data = line


        try:

            # 预编译对应的正则
            self.logParse.getLogFormatByConfStr(line_data['log_format_str'],line_data['log_format_vars'], line_data['log_format_name'])

            line_data['line'] = line_data['line'].strip()

            parse_data = self.logParse.parse(line_data['log_format_name'], line_data['line'])

        # 解析数据错误
        except ParseError as e:
            self.logging.error('\n pid : %s 数据解析错误: %s 数据: %s' % (os.getpid(), e.args, line))
            return False
        except ReCompile as e:
            unkown_error = '\n pid : %s 预编译错误: %s ,error_class: %s ,数据: %s' % (os.getpid(), e.__class__, e.args, line)
            self.logging.error(unkown_error)
            raise Exception(unkown_error)

        except Exception as e:
            unkown_error = '\n pid : %s 未知错误: %s ,error_class: %s ,数据: %s' % (os.getpid(), e.__class__, e.args, line)
            self.logging.error(unkown_error)
            raise Exception(unkown_error)


        del line_data['log_format_name']
        del line_data['log_format_str']
        del line_data['log_format_vars']
        del line_data['line']

        line_data.update(parse_data)


        return line_data


    def _get_queue_count_num(self):
        return self.queue_handle.getDataCountNum()


    #　获取队列数据
    def getQueueData(self):
        return self.queue_handle.getDataFromQueue()

    # 消费队列
    def saveToStorage(self ):
        self.storage_handle.pushDataToStorage()

    def watchTraffic(self):

        self.traffic_analysis_handel.start()

    #退回队列
    def rollBackQueue(self,data_list):
        self.queue_handle.rollBackToQueue(data_list)




if __name__ == "__main__":
    pass

