# coding=UTF-8
from multiprocessing import Queue,Process
from redis import Redis,exceptions
from configparser import ConfigParser
from threading import Thread,RLock
from pymongo import MongoClient
import time,shutil,json,subprocess,traceback,mmap,os,collections,platform,copy,importlib,sys,threading,pwd


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

    def _getQueue(self):

        client_queue_type = self.conf['client']['queue']

        if client_queue_type == 'redis':
            try:
                return Redis(
                    host = self.conf[client_queue_type]['host'],
                    port = int(self.conf[client_queue_type]['port']),
                    password = str(self.conf[client_queue_type]['password']),
                    db = self.conf[client_queue_type]['db']
                )

            except exceptions.RedisError as e:
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

        self.node_id = self.conf['client']['node_id']
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



        self.queue_key = self.conf['redis']['prefix'] + 'logger'

        self.server_conf = loggerParse(log_file_conf['server_type'],self.conf[log_file_conf['server_type']]['server_conf']).logger_format


        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()




    def __getFileFd(self):
        try:
            return open(self.log_path, 'rb+')
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
        print(';;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s' % (
            round(end_time - start_time, 2)))


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
                    print('cut_file_type: filesize ;%s ---pid: %s----thread_id: %s--- %s ---------%s' % (
                        now, os.getpid(), threading.get_ident(), self.cut_file_point, self.cutting_file))

                    # 文件大小 单位 M
                    file_size = round(os.path.getsize(self.log_path) / (1024 * 1024))
                    if file_size < int(self.cut_file_point):
                        self.lock.release()
                        continue

                except FileNotFoundError as e:
                    self.event['stop'] = self.log_path + '文件不存在'
                    continue

                self.__cutFileHandle(server_pid_path , self.log_path , target_path = self.cut_file_save_dir)


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


        try:
            redis = self._getQueue()
            pipe = redis.pipeline()
        except exceptions.RedisError  as e:
            self.event['stop'] = 'redis 链接失败:' +  e.args[1]


        while True:
            time.sleep(0.5)

            if self.event['stop']:
                print( '%s ; read threading stop pid: %s' % (self.event['stop'] ,os.getpid()))
                return

            start_time = time.perf_counter()
            print("\n start_time -------pid: %s -- read file---queue len: %s---- %s \n" % (os.getpid() ,redis.llen(self.queue_key), start_time))


            self.lock.acquire()

            for line in self.fd:

                data = {}
                data['node_id'] = self.node_id
                data['app_name'] = self.app_name
                data['log_format_name'] = self.log_format_name
                data['line'] = line.decode(encoding='utf-8').strip()
                try:
                    data['log_format_str'] = self.server_conf[self.log_format_name].strip()
                    data = json.dumps(data)
                    pipe.lpush(self.queue_key, data)

                except KeyError as e:
                    self.event['stop'] = self.log_format_name + '日志格式不存'
                    break




            pipe.execute()

            self.lock.release()

            end_time = time.perf_counter()
            print("\n end_time -------pid: %s -- read file---queue_len :%s ----%s 耗时:%s \n"
                  % (os.getpid(),redis.llen(self.queue_key), end_time, round(end_time - start_time, 2)))


            if self.event['cut_file'] == 1 and self.event['stop'] == None:
                print('--------------------reopen file--------------------')
                self.fd.close()
                self.fd = self.__getFileFd()
                self.fd.seek(0)
                self.event['cut_file'] = 0


    def runMethod(self,method_name):
        print('pid:%s , %s ,%s' % (os.getpid() ,method_name  ,time.perf_counter()))
        getattr(self,method_name)()



# 消费者 解析日志 && 存储日志
class OutputCustomer(Base):

    def __init__(self ):
        super(OutputCustomer,self).__init__()

        self.client_queue = self._getQueue()
        self.client_queue_prefix = self.conf[ self.conf['custom']['queue'] ]['prefix']
        self.client_queue_key = self.client_queue_prefix + 'logger'


        self.save_engine_conf = dict( self.conf[ self.conf['custom']['save_engine'] ])
        self.save_engine_name = self.conf['custom']['save_engine'].lower().capitalize()


        self.call_engine = 'saveTo%s' %  self.save_engine_name
        self.server_type = self.conf['custom']['log_server_type']

        self.logParse = loggerParse(self.conf['custom']['log_server_type'] ,server_conf=None)

        if not hasattr(self , self.call_engine ):
            raise ValueError('Outputer 未定义 "%s" 该存储方法' %  self.save_engine_name)



    def getQueueData(self):

        betch_max_size = int(self.conf['custom']['batch_insert_queue_max_size'])

        start_time = time.perf_counter()
        # print("\n customer -------pid: %s -- take from queue len: %s---- start \n" % (
        #     os.getpid(), self.client_queue.llen(self.client_queue_key)))

        pipe = self.client_queue.pipeline()
        for i in range(betch_max_size):
            pipe.lpop(self.client_queue_key)
        queue_list = pipe.execute()

        end_time = time.perf_counter()
        # print("\n customer -------pid: %s -- take from  queue len: %s----end 耗时: %s \n" % (
        #     os.getpid(), len(queue_list), round(end_time - start_time, 2)))

        return queue_list


    def parse_time_str(self,data):
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

    def parse_line_data(self,line):

        line_data = json.loads(line)

        # 预编译对应的正则
        self.logParse.getLogFormatByConfStr(line_data['log_format_str'] ,line_data['log_format_name'])

        try:
            line_data['line'] = line_data['line'].strip()

            # parse_data = self.logParse.parse(parse_data, line_data['line'])
            parse_data = self.logParse.parse(line_data['log_format_name'], line_data['line'])

        except Exception as e:
            self.client_queue.append(line)
            traceback.print_exc()
            exit()

        parse_data = self.parse_time_str(parse_data)

        del line_data['log_format_name']
        del line_data['log_format_str']
        del line_data['line']

        line_data.update(parse_data)

        return line_data


    def saveToMongodb(self):

        try:
            # Python 3.x
            from urllib.parse import quote_plus
        except ImportError:
            # Python 2.x
            from urllib import quote_plus

        if self.save_engine_conf['username'] and self.save_engine_conf['password']:
            mongo_url = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                (
                    self.save_engine_conf['username'],
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


        while True:
            time.sleep(1)
            num = self.client_queue.llen(self.client_queue_key)

            if num == 0:
                print('pid: %s wait for data' % os.getpid())
                continue

            insertList = []


            queue_list = self.getQueueData()


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
                line_data = self.parse_line_data(line)
                insertList.append(line_data)

            end_time = time.perf_counter()
            print("\n customer -------pid: %s -- reg datas len: %s---- end 耗时: %s \n" % (
                os.getpid(), len(insertList), round(end_time - start_time, 2)))


            if len(insertList):
                start_time = time.perf_counter()
                # print("\n customer -------pid: %s -- insert into mongodb: %s---- start \n" % (
                #     os.getpid(), len(insertList)))

                res = mongodb_client.insert_many(insertList, ordered=False)

                insertList = []

                end_time = time.perf_counter()
                # print("\n customer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (os.getpid(), len(res.inserted_ids), round(end_time - start_time ,2) ))




def startOutput(share_queue ):
    obj = OutputCustomer(share_queue=share_queue)
    getattr(obj,obj.call_engine)()



if __name__ == "__main__":
    pass

