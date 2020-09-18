from multiprocessing import Queue,Process
from multiprocessing.managers import BaseManager
from redis import Redis,RedisError
# from src.logger import loggerParse
from threading import Thread,ThreadError,RLock
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy



# redis = Redis(host='127.0.0.1',db=1)

# pipe = redis.pipeline(transaction=True)


# queue = Queue()

# #
# m = BaseManager(address=('127.0.0.1',5656),authkey=b'123123')
# #
# m.connect()
#
# m.register('get_queue')
# m.register('get_deque')
# q = m.get_queue()
# dq = m.get_deque()







# async def getRedisConnect():
#      redis_pool =
#      return redis_pool

class Reader(object):

    def __init__(self,log_path ,queue ):
        self.log_path = log_path
        self.queue = queue
        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()


    def __getFileFd(self):
        return open(self.log_path ,'rb+')
        # return fileinput.input(self.log_path )

    def full_cut(self):

        while True:
            time.sleep(1)
            start_time = time.perf_counter()
            print("\n start_time -------cutting file start --- queue_len:%s---- %s \n" % ( len(list(self.queue)) ,start_time) )

            if len(list(self.queue)) < 100000:
            # if len(list(self.queue)) < 4000:
                continue


            # self.lock.acquire(blocking=True)

            reader_queue = self.queue
            self.lock.acquire(blocking=True)

            # 清空文件
            self.fd.seek(0)
            self.fd.truncate()

            self.lock.release()
            print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s' % time.perf_counter())
            time.sleep(1) # 让出线程 让reader 去读
            # 完成清空标记 结束
            # self.cutting_file = True

            print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;full_cut;;;;;;;;;;;;;;;;;;; mark at %s' % time.perf_counter())
            # time.sleep(1)  # 阻塞1秒 供线程切换


            print('>>>>>>>>>>>>>>> %s <<<<<<<<<<<<<' % len(list(reader_queue)))

            # 创建新文件 &  移除队列前面
            end_time = time.perf_counter()
            print("\n end_time -------cutting file end truncate------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))


            file_suffix = time.strftime('%Y_%m_%d', time.localtime())
            # size = round(os.path.getsize(self.log_path) / 1024)

            queue_list = list(reader_queue)
            print('>>>>>>>>>>>>>>>reader_queue %s  >>>> self_queue len: %s ' % ( len(queue_list) ,len(list(self.queue)) ) )

            split_file = self.log_path + '_' + file_suffix
            split_content = []

            # 移除队列
            for i in range(len(queue_list)):
                split_content.append(self.queue.popleft()[1])


            end_time = time.perf_counter()
            print("\n end_time--queue len: %s-----cutting file end create new file------- %s 耗时： %s \n"
                  % ( len(list(self.queue)), end_time, round(end_time - start_time, 2)))


            # 写入新的切割文件
            with open(split_file, 'wb+') as split_file_fd:
                split_file_fd.writelines(split_content)

            del queue_list
            del split_content
            reader_queue = None


            end_time = time.perf_counter()
            print("\n end_time -------chunckFile------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))



    def startRead(self):
        try:
            position = 0
            self.fd.seek(position,0)

            while True:
                time.sleep(1)
                start_time = time.perf_counter()
                print("\n start_time -------read file---queue len: %s---- %s \n" % (len(list(self.queue)) ,start_time) )

                # if self.cutting_file == True:
                #     print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;reader01;;;;;;;;;;;;;;;;;;; reload at: %s' % time.perf_counter())
                #     # self.fd.close()
                #     # self.fd = self.__getFileFd()
                #     self.fd.seek(0)
                #     self.cutting_file = False

                self.lock.acquire(blocking=True)
                for line in self.fd:
                    position = self.fd.tell()
                    line = (position ,line)
                    self.queue.append(line)

                self.lock.release()

                queue_len = len(list(self.queue))
                # print(queue_len) # 97442 102552
                # print(queue_len) # 93616 106302
                # print(queue_len) # 96237 103672
                # print(queue_len) # 96237 103672

                end_time = time.perf_counter()
                print("\n end_time -------read file---queue_len :%s ----%s 耗时:%s \n" % (queue_len,end_time, round(end_time - start_time, 2)))


        finally:
            print('close the fd')
            self.fd.close()


    # 对半切
    def half_cut(self,queue_len):
        start_time = time.perf_counter()
        print("\n start_time -------chunckFile------- %s \n" % start_time)
        # logger_parse = loggerParse(server_type='Nginx',log_path=self.log_path)
        # print(loggerParse.logFormat)
        file_suffix = time.strftime('%Y_%m_%d', time.localtime())
        # size = round(os.path.getsize(self.log_path) / 1024)
        # last_line = list(self.queue)[-1]
        # print(today)
        # print('file size %s KB ' % size)
        # print('last line %s ' % last_line)

        # copy_queue = copy.deepcopy(self.queue)

        half = round(queue_len / 2)
        up = list(self.queue)[0:half]
        down = list(self.queue)[half:]

        print(len(up))
        print(type(up))
        print(up[-1])
        print(len(down))
        print(type(down))
        print(down[-1])

        split_file = self.log_path + '_' + file_suffix
        split_content = []
        for i in range(len(up)):
            split_content.append(up.pop()[1])

        # 翻转 保持时间正序
        split_content.reverse()
        with open(split_file, 'wb+') as split_file_fd:
            split_file_fd.writelines(split_content)

        # with open(self.log_path, 'rb+') as fd:
        #     fd.seek(324)
        #     print(fd.truncate(324))
        #
        #

        end_time = time.perf_counter()
        print("\n end_time -------chunckFile------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))
        # lines = list(copy_queue)[0:3]
        # for l in lines:
        #     position = l.split('__pos__')[0]
        #     with open(self.log_path,'rb+') as fd:
        #         fd.seek(int(position))
        #         print(position)
        # print(fd.truncate())

        # half_point = round( len(list(copy_queue))/2 )
        # # list(copy_queue)[0:1050540]
        # print(half_point)



    def run(self,method_name):
        print('%s ,%s' % (method_name  ,time.perf_counter()))
        getattr(self,method_name)()


    def __exit__(self, exc_type, exc_val, exc_tb):
        print('123123123')
        self.fd.close()




if __name__ == "__main__":


    log = '/www/wwwlogs/local.test.com.log'
    # log = '/www/wwwlogs/local.test.com.log_2020_09_18'
    # queue = Queue()
    queue = collections.deque()

    s_time = time.perf_counter()
    r = Reader(log_path=log,queue=queue)


    jobs = ['startRead','full_cut']
    # jobs = ['startRead']
    t = []
    for i in jobs:
        t.append(Thread(target=r.run ,args=(i,)) )


    for i in t:
        i.start()

    for i in t:
        i.join()
