# coding=UTF-8
from multiprocessing import Queue,Process
from redis import Redis,RedisError
from configparser import RawConfigParser
from threading import Thread,RLock
from pymongo import MongoClient
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy,importlib,sys,threading


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )





class Base(object):
    conf = None

    def __init__(self):
        self._root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.conf = self.__getConfig()

    def __getConfig(self):
        config_path = self._root + '/config.ini'
        if ( not os.path.exists(config_path) ):
            raise FileNotFoundError('config.ini not found in the project root path')

        conf = RawConfigParser()
        conf.read(config_path, encoding="utf-8")

        return conf

    def _getQueue(self):


        if self.conf['client.queue']['type'] == 'redis':
            try:
                return Redis(
                    host = self.conf['client.queue']['host'],
                    port = int(self.conf['client.queue']['port']),
                    password = str(self.conf['client.queue']['password']),
                    db = self.conf['client.queue']['db']
                )

            except RedisError as e:
                self.event['stop'] = e.args
        else:
            try:
                raise ValueError('当前只支持 redis 输出')
            except ValueError as e:
                self.event['stop'] = e.args

# 日志解析
class loggerParse(object):


    def __init__(self ,server_type,server_conf = None):

        self.server_type = server_type
        self.__handler = self.__findHandlerAdapter(server_type)()
        self.format = self.__handler.getLogFormat()
        if server_conf:
            self.logger_format = self.__handler.getLoggerFormatByServerConf(server_conf_path=server_conf)


    def getLogFormatByConfStr(self,log_format_conf):

        return  self.__handler.getLogFormatByConfStr( log_format_conf,'string')


    def parse(self,log_format,log_line):
        return  self.__handler.parse(log_format=log_format,log_line=log_line)



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

    def __init__(self,log_file_conf = None ,share_queue = None):
        super(Reader, self).__init__()
        self.log_path = log_file_conf['file_path']
        if len(log_file_conf['log_format_name']) == 0:
            self.log_format_name = 'defualt'
        else:
            self.log_format_name = log_file_conf['log_format_name']

        self.app_name = log_file_conf['app_name']
        self.read_type = log_file_conf['read_type']
        self.cut_file_type = log_file_conf['cut_file_type']
        self.cut_file_point = log_file_conf['cut_file_point']


        self.queue_key = self.conf['client.queue']['prefix'] + 'logger'
        self.server_conf = loggerParse(self.conf['server_info']['server_type'],self.conf['server_info']['server_conf']).logger_format



        self.share_queue = share_queue

        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()



    def __getFileFd(self):
        try:
            return open(self.log_path, 'rb+')
        except FileNotFoundError as e:
            self.event['stop'] = e.args


    def cutFile(self):

        while True:
            time.sleep(1)
            if self.event['stop']:
                print(self.event['stop'])
                return

            try:
                # 文件大小 单位 M
                file_size = round(os.path.getsize(self.log_path) / (1024 * 1024))
                if file_size < 20:
                    continue
            except FileNotFoundError as e:
                self.event['stop'] = e.args

            start_time = time.perf_counter()
            print("\n start_time -------cutting file start --- queue_len:%s---- %s \n" % (self.share_queue.qsize(), start_time))

            # 清空文件
            self.lock.acquire(blocking=True)

            file_suffix = time.strftime('%Y_%m_%d_%s', time.localtime())
            target_file = self.log_path + '_' + file_suffix

            server_pid_path = self.conf['server']['nginx_pid_path']
            if not os.path.exists(server_pid_path) :
                raise FileNotFoundError('配置项 server nginx_pid_path 不存在')

            cmd = 'mv %s %s && kill -USR1 `cat %s`' % (self.log_path,target_file,server_pid_path)
            res = os.popen(cmd)
            # print(res.readlines())

            end_time = time.perf_counter()
            print(';;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s' % (round(end_time - start_time, 2)))


            self.event['cut_file'] = 1

            self.lock.release()


    def readLog(self):


        if self.event['stop'] :
            print(self.event['stop'])
            return

        position = 0
        self.fd.seek(position, 0)

        redis = self._getQueue()
        pipe = redis.pipeline()

        while True:
            time.sleep(0.5)

            if self.event['stop']:
                print(self.event['stop'])
                return

            start_time = time.perf_counter()
            # print("\n start_time -------read file---queue len: %s---- %s \n" % (len(list(self.reader_queue)) ,start_time) )
            print("\n start_time -------pid: %s -- read file---queue len: %s---- %s \n" % (os.getpid() ,redis.llen(self.queue_key), start_time))

            self.lock.acquire()
            for line in self.fd:

                data = {}
                data['node_id'] = self.conf['client.input']['node_id']
                data['app_name'] = self.app_name
                data['log_format_name'] = self.log_format_name
                data['line'] = line.decode(encoding='utf-8')
                data['server_conf'] = self.server_conf
                data = json.dumps(data)

                pipe.lpush(self.queue_key,data)
                # self.share_queue.put(line)

            pipe.execute()
            self.lock.release()

            end_time = time.perf_counter()
            # print("\n end_time -------read file---queue_len :%s ----%s 耗时:%s \n"
            #       % (len(list(self.reader_queue)),end_time, round(end_time - start_time, 2)))
            print("\n end_time -------pid: %s -- read file---queue_len :%s ----%s 耗时:%s \n"
                  % (os.getpid(),redis.llen(self.queue_key), end_time, round(end_time - start_time, 2)))

            if self.event['cut_file'] == 1 and self.event['stop'] == None:
                self.fd.close()
                self.fd = self.__getFileFd()
                self.fd.seek(0)
                self.event['cut_file'] = 0


    def runMethod(self,method_name):
        print('%s ,%s' % (method_name  ,time.perf_counter()))
        getattr(self,method_name)()



# 消费者 解析日志 && 存储日志
class OutputCustomer(Base):

    def __init__(self ):
        super(OutputCustomer,self).__init__()

        self.client_queue = self._getQueue()
        self.client_queue_key = self.conf['client.queue']['prefix'] + 'logger'
        self.client_queue_prefix = self.conf['client.queue']['prefix']


        self.save_engine = self.conf['custom.save_engine']['type'].lower().capitalize()
        self.call_engine = 'saveTo%s' %  self.save_engine

        self.logParse = loggerParse(self.conf['server_info']['server_type'] ,server_conf=None)



        if not hasattr(self , self.call_engine ):
            raise ValueError('Outputer 未定义 "%s" 该存储方法' %  self.save_engine)



    def getClientQueueKeys(self):
        queue = self.client_queue
        _list = queue.keys(self.client_queue_prefix + ':logger')
        _list = list(map(lambda x:x.decode(encoding='utf-8'),_list))
        return _list



    def saveToMongodb(self):

        try:
            # Python 3.x
            from urllib.parse import quote_plus
        except ImportError:
            # Python 2.x
            from urllib import quote_plus

        if len(self.conf['custom.save_engine']['user']) and len(self.conf['custom.save_engine']['password']):
            mongo_url = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                (
                    quote_plus(self.conf['custom.save_engine']['user']),
                    quote_plus(self.conf['custom.save_engine']['password']),
                    self.conf['custom.save_engine']['host'],
                    int(self.conf['custom.save_engine']['port']),
                    quote_plus(self.conf['custom.save_engine']['db'])
                )
        else:
            mongo_url = 'mongodb://%s:%s/?authSource=%s' % \
                        (
                            self.conf['custom.save_engine']['host'],
                            int(self.conf['custom.save_engine']['port']),
                            quote_plus(self.conf['custom.save_engine']['db'])
                        )


        mongodb = MongoClient( mongo_url)
        mongodb_client = mongodb[ self.conf['custom.save_engine']['db'] ][ self.conf['custom.save_engine']['collection'] ]


        while True:
            time.sleep(1)
            num = self.client_queue.llen(self.client_queue_key)

            if num == 0:
                print('pid: %s wait for data' % os.getpid())
                continue

            betch_max_size = int(self.conf['custom']['batch_insert_queue_max_size'])
            insertList = []

            start_time = time.perf_counter()
            print("\n customer -------pid: %s -- take from queue len: %s---- start \n" % (os.getpid(), self.client_queue.llen(self.client_queue_key)))

            pipe = self.client_queue.pipeline()
            for i in range(betch_max_size):
                pipe.lpop(self.client_queue_key)
            queue_list = pipe.execute()


            end_time = time.perf_counter()
            print("\n customer -------pid: %s -- take from  queue len: %s----end 耗时: %s \n" % (
            os.getpid(), len(queue_list), round(end_time - start_time, 2)))


            if len(queue_list) == 0:
                print('pid: %s wait for data' % os.getpid())
                continue

            start_time = time.perf_counter()
            print("\n customer -------pid: %s -- reg data len: %s---- start \n" % (
                os.getpid(), len(queue_list)))


            for i in queue_list:
                if not i :
                    continue

                line = i.decode(encoding='utf-8')

                if line:
                    line_data = json.loads(line)
                    server_conf = line_data['server_conf']

                    parse_data = self.logParse.getLogFormatByConfStr( server_conf[line_data['log_format_name']] )

                    try:
                        line_data['line'] = line_data['line'].strip()

                        parse_data = self.logParse.parse(parse_data, line_data['line'])

                    except Exception as e:
                        self.client_queue.append(line)
                        traceback.print_exc()
                        print(e.args)
                        exit()



                    del line_data['server_conf']
                    del line_data['line']

                    line_data.update(parse_data)

                    insertList.append(line_data)

            end_time = time.perf_counter()
            print("\n customer -------pid: %s -- reg datas len: %s---- end 耗时: %s \n" % (
                os.getpid(), len(insertList), round(end_time - start_time, 2)))



            if len(insertList):
                start_time = time.perf_counter()
                print("\n customer -------pid: %s -- insert into mongodb: %s---- start \n" % (
                    os.getpid(), len(insertList)))

                res = mongodb_client.insert_many(insertList, ordered=False)

                insertList = []

                end_time = time.perf_counter()
                print("\n customer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (os.getpid(), len(res.inserted_ids), round(end_time - start_time ,2) ))




def startOutput(share_queue ):
    obj = OutputCustomer(share_queue=share_queue)
    getattr(obj,obj.call_engine)()



def startReader(log_path ,share_queue):
    r = Reader(log_path=log_path, share_queue=share_queue)
    r.output_process = Process(target=startOutput ,args=(queue,) )

    jobs = ['readLog', 'cutFile', 'watcher']
    t = []
    for i in jobs:
        th = Thread(target=r.runMethod, args=(i,))
        th.setDaemon(True)
        t.append(th)

    for i in t:
        i.start()

    for i in t:
        i.join()




if __name__ == "__main__":

    log = '/www/wwwlogs/local.test.com.log'
    # log = '/www/wwwlogs/local.test.com.log_2020_09_18'
    queue = Queue()


    # with open(log,'rb+') as fd:
    #     for line in fd:
    #         queue.put(line)

    # startOutput(share_queue=queue)


    startReader(log_path=log, share_queue=queue)
