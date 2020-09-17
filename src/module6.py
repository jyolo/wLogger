from multiprocessing import Queue,Process
from multiprocessing.managers import BaseManager
from redis import Redis,RedisError
# from src.logger import loggerParse
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy,fileinput



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


    def __getFileFd(self):
        return open(self.log_path ,'rb+')
        # return fileinput.input(self.log_path )


    def full_cut(self):

        start_time = time.perf_counter()
        print("\n start_time -------chunckFile------- %s \n" % start_time)

        with open(self.log_path,'rb+') as fd:
            fd.seek(0)
            fd.truncate()

        file_suffix = time.strftime('%Y_%m_%d', time.localtime())
        # size = round(os.path.getsize(self.log_path) / 1024)
        queue_list = list(self.queue)
        print(len(queue_list))
        # print(today)
        # print('file size %s KB ' % size)
        # print('last line %s ' % last_line)

        last_line_seek = queue_list[-1][0]
        print(last_line_seek)

        split_file = self.log_path +'_'+ file_suffix
        split_content = []


        for i in range(len(queue_list)):
            # self.queue.popleft()  # 从左边开始 清空队列 和 queue_list 相同的数据
            # split_content.append(queue_list[i][1])
            split_content.append( self.queue.popleft()[1] )

        end_time = time.perf_counter()
        print("\n end_time -------chunckFile------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))
        del queue_list
        print(len(list(self.queue)))
        print(len(split_content))
        # # 翻转 保持时间正序
        split_content.reverse()
        with open(split_file ,'wb+') as split_file_fd:
            split_file_fd.writelines(split_content)

        # with open(self.log_path, 'rb+') as fd:
        #     fd.seek(324)
        #     print(fd.truncate(324))
        #
        #

        end_time = time.perf_counter()
        print("\n end_time -------chunckFile------- %s 耗时： %s \n" % (end_time , round(end_time - start_time,2)) )
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


    def startRead(self):
        try:
            position = 0
            self.fd.seek(position,0)
            # 文件切割中
            self.cutting_file = False

            while True:
                time.sleep(1)
                start_time = time.perf_counter()
                print("\n start_time -------read file------- %s \n" % start_time)
                for line in self.fd:
                    position = self.fd.tell()
                    line = (position ,line)
                    self.queue.append(line)


                if self.cutting_file == True:
                    self.fd.close()
                    self.fd = self.__getFileFd()
                    self.cutting_file = False
                    self.startRead()



                queue_len = len(list(self.queue))
                print(queue_len)
                if queue_len == 35 and self.cutting_file == False:
                    self.cutting_file = True
                    print('full_cut')
                    self.full_cut()
                    # self.half_cut(queue_len)
                    pass

                end_time = time.perf_counter()
                print("\n end_time -------read file-------%s 耗时:%s \n" % (end_time, round(end_time - start_time, 2)))
                # break

        finally:
            print('close the fd')
            self.fd.close()

    def getQueue(self):
        return self.queue

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('123123123')
        self.fd.close()



def starRead(log_path,queue):
    postion = 0

    while True:

        time.sleep(1)

        start_time = time.time()
        print('read position : %s start at: %s' % (postion, start_time))

        with open(log_path, 'rb') as fd:

            if (postion > 0):
                # print('------------read position : %s-------------' % postion)
                fd.seek(postion)

            for line in fd:
                # linestr = line.decode(encoding='utf-8')
                if (len(line) == 0):
                    continue

                postion = fd.tell()
                # queue.put(linestr)
                queue.append(line)
                break

            end_time = time.time()
            print('耗时: %s 队列长度 deque: %s' % (end_time - start_time, len(list(queue))))
            # print('耗时: %s 队列长度 deque: %s' % (end_time - start_time, queue.qsize()))




if __name__ == "__main__":


    log = '/www/wwwlogs/local.test.com.log'
    # queue = Queue()
    queue = collections.deque()

    s_time = time.perf_counter()
    r = Reader(log_path=log,queue=queue)
    r.startRead()
    # print('队列总长: %s ;耗时: %s' % (len(list(r.queue)) , (time.perf_counter() - s_time ) )  )

