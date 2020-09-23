# coding=UTF-8
from multiprocessing import Queue,Process
from redis import Redis,RedisError
from configparser import ConfigParser
from threading import Thread,RLock
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy,importlib,sys


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

        conf = ConfigParser()
        conf.read(config_path, encoding="utf-8")

        return conf


class loggerParse(object):


    def __init__(self ,server_type,server_conf):

        self.server_type = server_type
        self.__handler = self.__findHandlerAdapter(server_type)()
        self.format = self.__handler.getLogFormat()
        self.logger_format = self.__handler.getLoggerFormatByServerConf(server_conf_path=server_conf)


    # def parse(self,log_format='',log_line=''):
    #
    #     self.__handler.parse(log_format='',log_line='')
    #     pass


    #  动态调用handel方法 and 给出友好的错误提示
    def __getattr__(self, item ,**kwargs):

        if hasattr(self.__handler,item):
            def __callHandleMethod(**kwargs):
                if(list(kwargs.keys()) != ["log_format", "log_line"]) and item == 'parse':
                    raise ValueError('%s handle %s 方法:需要两个kwargs (log_format="", log_line="")' % (self.server_type, item))

                if (list(kwargs.keys()) != ["log_conf", "log_type"]) and item == 'getLogFormatByConfStr':
                    raise ValueError('%s handle %s 方法:需要两个kwargs (log_conf="", log_type="")' % (self.server_type, item))


                return getattr(self.__handler,item)(**kwargs)
        else:
            raise ValueError('%s handle 没有 %s 方法'  % (self.server_type,item) )

        return __callHandleMethod

    def __findHandlerAdapter(self,server_type):
        try:
            handler_module = 'ParserAdapter.%s' % server_type.lower().capitalize()
            return importlib.import_module(handler_module).Handler
        except ModuleNotFoundError:
            raise ValueError('server_type %s not found' % server_type)




class Reader(Base):


    event = {
        'cut_file' : 0,
        'stop' : 0,
    }

    def __init__(self,log_file_conf = None ,share_queue = None):
        super(Reader, self).__init__()
        self.log_path = log_file_conf['file_path']
        self.log_format_name = log_file_conf['log_format_name']
        self.read_type = log_file_conf['read_type']
        self.cut_file_type = log_file_conf['cut_file_type']
        self.cut_file_point = log_file_conf['cut_file_point']

        self.share_queue = share_queue
        self.queue = collections.deque()

        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()


    def __getRedis(self):
        # return Redis(host='127.0.0.1', db=1)
        return Redis(host='120.78.248.191',password='xiaofeibao@DEVE#redis%PASSWD*2018^',port=35535, db=1)


    def __getFileFd(self):
        return open(self.log_path ,'rb+')


    def cutFile(self):

        while True:
            time.sleep(1)
            if self.event['stop'] == 1:
                return 's地方地方收水电费水电费111111111'

            # 文件大小 单位 M
            file_size = round(os.path.getsize(self.log_path) / (1024 * 1024))
            if file_size < 20:
                continue

            start_time = time.perf_counter()
            print("\n start_time -------cutting file start --- queue_len:%s---- %s \n"
                  % (self.share_queue.qsize(), start_time))

            # 清空文件
            self.lock.acquire(blocking=True)

            file_suffix = time.strftime('%Y_%m_%d_%s', time.localtime())
            target_file = self.log_path + '_' + file_suffix

            cmd = 'mv %s %s && kill -USR1 `cat /www/server/nginx/logs/nginx.pid`' % (self.log_path,target_file)
            res = os.popen(cmd)
            # print(res.readlines())

            print(self.share_queue.qsize())
            print(len(list(self.reader_queue)))

            end_time = time.perf_counter()
            print(';;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s'
                  % (round(end_time - start_time, 2)))


            self.event['cut_file'] = 1
            # 完成清空标记 结束
            self.lock.release()




    def readLog(self):

        position = 0
        self.fd.seek(position, 0)

        # redis = self.__getRedis()
        # pipe = redis.pipeline()

        while True:
            time.sleep(0.5)

            if self.event['stop'] == 1:
                return '爱仕达撒大所大所多111111111'

            start_time = time.perf_counter()
            # print("\n start_time -------read file---queue len: %s---- %s \n" % (len(list(self.reader_queue)) ,start_time) )
            print("\n start_time -------read file---queue len: %s---- %s \n" % (self.share_queue.qsize(), start_time))

            self.lock.acquire()
            for line in self.fd:
                # self.reader_queue.append(line)
                # pipe.lpush('log_line',line)
                self.share_queue.put(line)

            # pipe.execute()
            self.lock.release()

            end_time = time.perf_counter()
            # print("\n end_time -------read file---queue_len :%s ----%s 耗时:%s \n"
            #       % (len(list(self.reader_queue)),end_time, round(end_time - start_time, 2)))
            print("\n end_time -------read file---queue_len :%s ----%s 耗时:%s \n"
                  % (self.share_queue.qsize(), end_time, round(end_time - start_time, 2)))

            if self.event['cut_file'] == 1:
                self.fd.close()
                self.fd = self.__getFileFd()
                self.fd.seek(0)
                self.event['cut_file'] = 0


    def watcher(self):
        p = self.output_process
        p.start()
        while True:
            time.sleep(1)
            msg = 'subprocess pid: %s ; is_alive: %s' % (p.pid , p.is_alive())
            print(msg)
            if p.is_alive() == False:
                self.event['stop'] = 1
                return

    def runMethod(self,method_name):
        print('%s ,%s' % (method_name  ,time.perf_counter()))
        getattr(self,method_name)()

class Outputer(Base):
    def __init__(self,share_queue,log_format_name ):
        super(Outputer,self).__init__()
        self.share_queue = share_queue
        self.save_engine = self.conf['client.output']['type'].lower().capitalize()
        self.call_engine = 'saveTo%s' %  self.save_engine

        self.logParse = loggerParse(self.conf['client.input']['server_type'] ,self.conf['client.input']['server_conf'])
        try:
            self.format_str = self.logParse.logger_format[log_format_name]
        except KeyError: # 找不到 则用默认
            self.format_str = self.logParse.logger_format['defualt']

        self.log_format_parse_str = self.logParse.getLogFormatByConfStr(log_conf = self.format_str ,log_type='string')



        if not hasattr(self , self.call_engine ):
            raise ValueError('Outputer 未定义 "%s" 该存储方法' %  self.save_engine)


    def saveToRedis(self):

        # log_files = eval(self.conf['client.input']['log_files'].strip())
        # print(log_files)

        while True:
            time.sleep(1)
            print('111111111')

            print(self.log_format_parse_str)
            line = self.share_queue.get()
            line = line.decode(encoding='utf-8')
            print(line)

            res = self.logParse.parse(log_format=self.log_format_parse_str[1],log_line= line)
            print(res)

        # redis = Redis(host='127.0.0.1', db=1)
        # # redis = Redis(host='120.78.248.191',password='xiaofeibao@DEVE#redis%PASSWD*2018^',port=35535, db=1)
        #
        # while True:
        #     time.sleep(1)
        #
        #     start_time = time.perf_counter()
        #     print("\n >>>>>>>>>>>>>>saveData---queue len: %s---- %s \n" % (self.queue.qsize(), start_time))
        #
        #     if queue.qsize() == 0:
        #         continue
        #
        #     pipe = redis.pipeline()
        #     for i in range(queue.qsize()):
        #         pipe.lpush('log_line', queue.get())
        #
        #     res = pipe.execute()
        #     print(' >>>>>>>>>>>>>>saveData---execute queue len: %s-' % len(res))
        #
        #     end_time = time.perf_counter()
        #     print("\n >>>>>>>>>>>>>>saveData---queue len: %s---- %s 耗时:%s\n" % (queue.qsize(), end_time ,round(end_time - start_time,2) ))





def startOutput(share_queue ):
    obj = Outputer(share_queue=share_queue)
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
