from multiprocessing import Queue,Process
from redis import Redis,RedisError
# from src.logger import loggerParse
from threading import Thread,ThreadError,RLock
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy



# redis = Redis(host='127.0.0.1',db=1)

# pipe = redis.pipeline(transaction=True)


class Reader(object):

    cutFileFlag = 0

    def __init__(self,log_path = None ,share_queue = None,save_engine = None):
        self.log_path = log_path
        self.share_queue = share_queue
        self.queue = collections.deque()
        self.reader_queue = copy.deepcopy(self.queue)
        self.outpuer_queue = copy.deepcopy(self.queue)
        self.outpuer_max_length = 10000
        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()
        self.save_engine = save_engine


    def __getRedis(self):
        # return Redis(host='127.0.0.1', db=1)
        return Redis(host='120.78.248.191',password='xiaofeibao@DEVE#redis%PASSWD*2018^',port=35535, db=1)


    def __getFileFd(self):
        return open(self.log_path ,'rb+')


    def cutFile(self):

        while True:
            time.sleep(1)

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

            self.cutFileFlag = 1
            # 完成清空标记 结束
            self.lock.release()
            # time.sleep(3) # 让出线程 让reader 去读

    def outputer(self):

        while True:
            time.sleep(1)

            reader_queue_len = len(list(self.reader_queue))

            start_time = time.perf_counter()
            print(
                "\n start_time -------outputer---queue len: %s---- %s \n" % (reader_queue_len, start_time))


            if reader_queue_len :
                for i in range(reader_queue_len):
                    item = self.reader_queue.pop()
                    self.share_queue.put(item)

            end_time = time.perf_counter()
            print("\n end_time -------outputer---queue_len :%s --share_queue_len--%s 耗时:%s \n"
                  % (reader_queue_len, self.share_queue.qsize() , round(end_time - start_time, 2) ))

    @staticmethod
    def saveWithParseLog(queue,engine):
        while True:
            time.sleep(1)

            start_time = time.perf_counter()
            print(
                "\n >>>>>>>>>>>>>>saveData---queue len: %s---- %s \n" % (queue.qsize(), start_time))

            if queue.qsize() == 0:
                continue

            if engine == 'redis' :

                redis = Redis(host='120.78.248.191',password='xiaofeibao@DEVE#redis%PASSWD*2018^',port=35535, db=1)
                pipe = redis.pipeline()
                for i in range(queue.qsize()):
                    pipe.lpush('log_line', queue.get())


                res = pipe.execute()
                print(' >>>>>>>>>>>>>>saveData---execute queue len: %s-' % len(res))

            elif engine == 'mongodb':
                pass
            elif engine == 'mysql':
                pass

            end_time = time.perf_counter()
            print(
                "\n >>>>>>>>>>>>>>saveData---queue len: %s---- %s 耗时:%s\n" % (queue.qsize(), end_time ,round(end_time - start_time,2) ))


    def startRead(self):

        try:
            position = 0
            self.fd.seek(position,0)

            # redis = self.__getRedis()
            # pipe = redis.pipeline()

            while True:
                time.sleep(0.5)

                start_time = time.perf_counter()
                # print("\n start_time -------read file---queue len: %s---- %s \n" % (len(list(self.reader_queue)) ,start_time) )
                print("\n start_time -------read file---queue len: %s---- %s \n" % (self.share_queue.qsize() ,start_time) )

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
                      % (self.share_queue.qsize(),end_time, round(end_time - start_time, 2)))


                if self.cutFileFlag == 1:
                    self.fd.close()
                    self.fd = self.__getFileFd()
                    self.fd.seek(0)
                    self.cutFileFlag = 0

        except Exception:
            traceback.print_exc()
            exit()


    def run(self,method_name):
        print('%s ,%s' % (method_name  ,time.perf_counter()))
        getattr(self,method_name)()




def startReader(log_path ,share_queue):


    r = Reader(log_path=log_path ,share_queue = share_queue )

    # jobs = ['startRead','cutFile','outputer']
    # jobs = ['startRead','outputer','cutFile']
    jobs = ['startRead','cutFile']
    t = []
    for i in jobs:
        th = Thread(target=r.run, args=(i,))
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


    pid = os.fork()
    if pid == 0:
        Reader.saveWithParseLog(queue=queue,engine='redis')
        pass
    else:
        startReader(log_path=log , share_queue = queue)
