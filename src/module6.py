from multiprocessing import Queue,Process
from multiprocessing.managers import BaseManager
from redis import Redis,RedisError
# from src.logger import loggerParse
from threading import Thread,ThreadError,RLock
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform,copy



# redis = Redis(host='127.0.0.1',db=1)

# pipe = redis.pipeline(transaction=True)


class Reader(object):

    def __init__(self,log_path ,queue ,share_memo = None):
        self.log_path = log_path
        self.queue = queue
        self.reader_queue = copy.deepcopy(queue)
        self.outpuer_queue = copy.deepcopy(queue)
        self.outpuer_max_length = 10000
        self.fd = self.__getFileFd()
        # 文件切割中标志
        self.cutting_file = False
        self.lock = RLock()
        self.redis = self.__getRedis()
        self.share_memo = share_memo


    def __getRedis(self):
        return Redis(host='127.0.0.1', db=1)

    def __getFileFd(self):
        return open(self.log_path ,'rb+')
        # return fileinput.input(self.log_path )

    def cutFile(self):


        while True:
            time.sleep(1)

            if len(list(self.reader_queue)) < 500000:
                continue

            start_time = time.perf_counter()
            print("\n start_time -------cutting file start --- queue_len:%s---- %s \n"
                  % (len(list(self.reader_queue)), start_time))


            # 清空文件
            self.lock.acquire(blocking=True)
            print('1111111111111111111111111111111111111111111111111')

            file_suffix = time.strftime('%Y_%m_%d', time.localtime())
            target_file = self.log_path + '_' + file_suffix

            self.fd.close()

            cmd = 'mv %s %s && kill -USR1 `cat /www/server/nginx/logs/nginx.pid`' % (self.log_path,target_file)
            res = os.popen(cmd)
            print(res.readlines())

            del self.reader_queue
            self.reader_queue = copy.deepcopy(self.queue)

            self.fd = self.__getFileFd()
            self.fd.seek(0)

            print(len(self.reader_queue))
            print('22222222222222222222222222222222222222222222222222222')
            end_time = time.perf_counter()
            print(';;;;;;;;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s'
                  % (round(end_time - start_time, 2)))


            # 完成清空标记 结束
            self.lock.release()
            time.sleep(3) # 让出线程 让reader 去读


            # end_time = time.perf_counter()
            # print(';;;;;;;reader_queue:%s ; sel.queue: %s ;;;;;;;;;full_cut;;;;;;finnish truncate;;;;;;;;;;;;; mark at %s'
            #       % (len(list(reader_queue)),len(list(self.queue)),round(end_time - start_time, 2)))
            # time.sleep(3) # 让出线程 让reader 去读


            # print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;full_cut;;;;;;;;;;;;;;;;;;; mark at %s' % time.perf_counter())
            # # time.sleep(1)  # 阻塞1秒 供线程切换
            #
            #
            # print('>>>>>>>>>>>>>>> %s <<<<<<<<<<<<<' % len(list(reader_queue)))
            #
            # # 创建新文件 &  移除队列前面
            # end_time = time.perf_counter()
            # print("\n end_time -------cutting file end truncate------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))
            #
            #
            # file_suffix = time.strftime('%Y_%m_%d', time.localtime())
            # # size = round(os.path.getsize(self.log_path) / 1024)
            #
            # queue_list = list(reader_queue)
            # print('>>>>>>>>>>>>>>>reader_queue %s  >>>> self_queue len: %s ' % ( len(queue_list) ,len(list(self.queue)) ) )
            #
            # split_file = self.log_path + '_' + file_suffix
            # split_content = []
            #
            # # 移除队列
            # for i in range(len(queue_list)):
            #     split_content.append(self.queue.popleft())
            #
            #
            # end_time = time.perf_counter()
            # print("\n end_time--queue len: %s-----cutting file end create new file------- %s 耗时： %s \n"
            #       % ( len(list(self.queue)), end_time, round(end_time - start_time, 2)))
            #
            #
            # # 写入新的切割文件
            # with open(split_file, 'wb+') as split_file_fd:
            #     split_file_fd.writelines(split_content)
            #
            # del queue_list
            # del split_content
            # reader_queue = None
            #
            #
            # end_time = time.perf_counter()
            # print("\n end_time -------chunckFile------- %s 耗时： %s \n" % (end_time, round(end_time - start_time, 2)))


    def outputer(self):
        Process

    def outpuer2(self):
        while True:
            time.sleep(1)
            start_time = time.perf_counter()
            print("\n start_time -------outpuer start --- queue_len:%s---- %s \n"
                  % (len(list(self.reader_queue)), start_time))

            pipe = self.redis.pipeline()

            send = False

            self.lock.acquire()

            for i in range(self.outpuer_max_length):
                try:
                    line = self.reader_queue.popleft()
                    self.outpuer_queue.append(line)
                    pipe.lpush('log_line', line)
                    if (len(list(self.outpuer_queue)) >= self.outpuer_max_length):
                        send = True
                        break

                # when reader_queue empty
                except IndexError:
                    break


            if send == True:
                print('start to send pipe data len : %s' % len(list(self.outpuer_queue)))
                pipe.execute()
                del self.outpuer_queue
                self.outpuer_queue = copy.deepcopy(self.queue)
                send = False

            self.lock.release()

            end_time = time.perf_counter()
            print("\n end_time -------outpuer end 发送数据 ----%s 耗时:%s \n"
                  % ( end_time, round(end_time - start_time, 2)))

    def startRead(self):
        try:
            position = 0
            self.fd.seek(position,0)

            while True:
                time.sleep(1)
                start_time = time.perf_counter()
                print("\n start_time -------read file---queue len: %s---- %s \n" % (len(list(self.reader_queue)) ,start_time) )


                self.lock.acquire()
                for line in self.fd:
                    self.reader_queue.append(line)
                    self.share_memo.write(line)
                    exit()
                self.lock.release()

                end_time = time.perf_counter()
                print("\n end_time -------read file---queue_len :%s ----%s 耗时:%s \n"
                      % (len(list(self.reader_queue)),end_time, round(end_time - start_time, 2)))

        except Exception:
            traceback.print_exc()
            exit()

        # finally:
        #     print('close the fd')
        #     self.fd.close()

    def run(self,method_name):
        print('%s ,%s' % (method_name  ,time.perf_counter()))
        getattr(self,method_name)()


    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     print('123123123')
    #     self.fd.close()


def outputerInSubProcess(data_queue):
    while True:
        time.sleep(1)
        print('------------------->>>>>>>>>>>>>>>>>>>>>>>.. outputer:pid %s ;datalen: %s' % (os.getpid() ,len(list(os.getpid())) ))


def startReader(log_path, queue ,share_memo):

    r = Reader(log_path=log, queue=queue ,share_memo=share_memo)

    # jobs = ['startRead','cutFile','outputer']

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
    # queue = Queue()
    queue = collections.deque()

    # p = Process(target=startReader,args=(log,queue) )
    # p.start()
    # p.join()

    share_memo = mmap.mmap(-1, 700)

    pid = os.fork()
    if pid == 0:
        while True:
            time.sleep(1)
            res = share_memo.readline().replace(b'\x00',b'')
            print(res)

            _list = b''.join(list(share_memo)).strip(b'\n')
            print(len(_list))
            print(_list)
            print(_list[0])

    else:
        startReader(log_path=log,queue=queue,share_memo=share_memo)

    # share_memo = mmap.mmap(-1,300)
    #
    # pid = os.fork()
    # if pid == 0:
    #     while True:
    #         time.sleep(3)
    #         print(len(list(share_memo)))
    #         print(b''.join(list(share_memo)).strip(b'\n').split(b'\n'))
    # else:
    #     for i in range(10):
    #         str = "A%s\n" % i
    #         a = share_memo.write(str.encode(encoding='utf-8'))
    #
    #     while True:
    #         time.sleep(1)


