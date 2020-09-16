from multiprocessing import Queue,Process
from multiprocessing.managers import BaseManager
from redis import Redis,RedisError
import time,asyncio,json,aioredis,traceback,mmap,os,collections,platform



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



def starRead(log_path,queue):
    postion = 0

    while True:

        time.sleep(1)

        with open(log_path, 'rb') as fd:

            if (postion > 0):
                # print('------------read position : %s-------------' % postion)
                fd.seek(postion)

            start_time = time.time()
            # print('read position : %s start at: %s' % (postion, start_time))

            for line in fd:
                linestr = line.decode(encoding='utf-8')
                if (len(linestr) == 0):
                    continue

                postion = fd.tell()
                queue.put(linestr)

            end_time = time.time()
            # print('耗时: %s 队列长度 deque: %s' % (end_time - start_time, len(list(deque))))
            # print('耗时: %s 队列长度 deque: %s' % (end_time - start_time, queue.qsize()))





async def sendLineToSever(queue ,task_num,each_task_handle_num,task_index):
    try:

        redis = await aioredis.create_redis_pool(
            address=('127.0.0.1',6379),
            maxsize=task_num,
            db=1
        )

        while True:
            await asyncio.sleep(1)
            start_time = time.time()


            # print('-----------sendLineToSever %s : %s 队列总长 : %s' % (task_index ,start_time, queue.qsize() ))
            pipe = redis.pipeline()

            if(queue.qsize() < each_task_handle_num):
                handle_num = queue.qsize()
            else:
                handle_num = each_task_handle_num


            for i in range(handle_num):
                if queue.empty():
                    break
                try:
                    lines = queue.get_nowait()
                    # lines = queue.get()
                except Exception :
                    break

                pipe.lpush('log_line', lines)



            handle_lines = await pipe.execute()

            end_time = time.time()
            print('+++pid:%s+++sendLineToSever %s 耗时: %s ---at:%s---- 处理数据: %s 队列总长 : %s' %
                  (
                    os.getpid(),
                    task_index,
                    int(end_time - start_time),
                    time.time(),
                    len(handle_lines),
                    queue.qsize()
                  )
              )
            # print('++++++++++++sendLineToSever %s 耗时: %s ------- 处理数据: ' % (task_index, int(end_time - start_time)))


    except Exception as e:
        traceback.print_exc()




async def subProcessStart(queue ,task_num,each_task_handle_num):
    task = []

    for i in range(task_num):
        task.append(asyncio.ensure_future(sendLineToSever(queue ,task_num,each_task_handle_num ,i)))

    done, pading = await asyncio.wait(task)
    print(pading)

def mainProcess(task_num=5,each_task_handle_num=5000 ,task_process=1):

    queue = Queue()

    p_list = []
    for i  in range(task_process):
        p_list.append(
            Process(target=subProcess ,args=(queue,task_num,each_task_handle_num,))
        )

    for p in p_list:
        p.start()

    # 2910231  2904112 2907578
    starRead('/www/wwwlogs/local.test.com.log', queue)

    # pid = os.fork()
    # if pid == 0:
    #     subProcess(queue,task_num,each_task_handle_num)
    #     pass
    # else:
    #     # while True:
    #     #     time.sleep(1)
    #     #     pass
    #     starRead('/www/wwwlogs/local.test.com.log', queue)
        # loop = asyncio.get_event_loop()
        # asyncio.run(start(deque))
        # loop.run_until_complete(start())

def subProcess(queue,task_num,each_task_handle_num):

    loop = asyncio.get_event_loop()
    asyncio.run(subProcessStart(queue,task_num,each_task_handle_num))
    loop.run_forever()

if __name__ == "__main__":

    # logPath = '/www/wwwlogs/local.test.com.log'
    #
    # starRead(logPath)



    mainProcess(
        task_num = 1,
        each_task_handle_num = 50000,
        task_process=4
    )
    # 4 个进程 1个协程 10000 2:19 平均每秒 4W
    # 4 个进程 1个协程 50000 2:37 平均每秒 4W
    # 4 个进程 4个协程 1:41 平均每秒 4W (数据丢失2910231 2901905)
    # 1 个进程 4个协程 4:14 平均每秒 2W




    # p = Process(target=subProcess ,args=(_deque,))
    # p.start()
    # p.join()
