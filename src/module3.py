from multiprocessing import Queue
from multiprocessing.managers import BaseManager
from multiprocessing import Process
from multiprocessing import Queue
from collections import deque
from redis import Redis,RedisError
import time,asyncio,json,aioredis,traceback



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

task_num = 10


async def getRedisConnect():
     redis_pool = await aioredis.create_redis_pool(
        address=('127.0.0.1',6379),
        maxsize=task_num,
        db=1
    )

     return redis_pool

async def starRead(log_path,_deque):
    postion = 0

    while True:

        await asyncio.sleep(1)

        try:

            # redis.ping()

            with open(log_path, 'rb') as fd:

                if (postion > 0):
                    print('------------read position : %s-------------' % postion)
                    fd.seek(postion)

                start_time = time.time()
                print('read position : %s' % postion)

                for line in fd:

                    line = line.decode(encoding='utf-8')
                    if (len(line) == 0):
                        continue

                    postion = fd.tell()
                    _deque.append(line)
                    # _queue.put(line)
                    # data = json.dumps({'position': postion,'line':line ,'file':log_path})
                    # pipe.lpush('log_line',data)

                    # for i in list(deque):
                    #     data = json.dumps({'position': postion,'line':i ,'file':log_path})

                    # redis 提交
                # pipe.execute()
                # pipe.close()
                # 记录当前的 position
                # postion = fd.tell()
                # print('------------read current position : %s-------------' % postion)
                # print(redis.llen('log_line'))

                end_time = time.time()
                print('耗时: %s 队列长度 deque: %s' % ( int(end_time - start_time), len(list(_deque)) ))


        except RedisError:
            # print('------------read current position : %s-------------' % fd.tell())
            print('redis connect error wait for redis-sever ')


each_task_handle_num = 100000

async def sendLineToSever(_deque):
    try:

        redis = await getRedisConnect()


        # total = await redis.llen('log_line')
        #
        # for i in range(total):
        #     line = await redis.lpop('log_line')
        #     if line:
        #         deque.append(line)
        #
        #
        # print(len(list(deque)))
        # return

        while True:
            await asyncio.sleep(1)



            start_time = time.time()

            if (len(list(_deque)) == 0):
                print('wait for deque data')
                continue

            print('-----------sendLineToSever  : %s 队列总长 : %s' % (start_time, len(list(_deque))))
            pipe = redis.pipeline()

            for i in range(each_task_handle_num):
                try:
                    lines = _deque.pop()
                    pipe.lpush('log_line', lines)
                except IndexError as e: # deque empty
                    break


            handle_lines = await pipe.execute()

            end_time = time.time()
            print('----------sendLineToSever开始时间:%s  耗时: %s ------- 处理数据: %s' % (
            start_time, int(end_time - start_time), len(handle_lines)))
            # print('----------sendLineToSever耗时: %s ------- 处理数据: %s' % ( (end_time - start_time) , len(list(deque)) ) )
    except Exception as e:
        traceback.print_exc()





async def start(_queue):
    task = []

    task.append(asyncio.ensure_future(starRead('/www/wwwlogs/local.test.com.log',_queue)))

    # for i in range(task_num):
    #     task.append(asyncio.ensure_future(sendLineToSever()))

    done, pading = await asyncio.wait(task)
    print(pading)

async def subProcessStart(_deque):
    task = []

    # task.append(asyncio.ensure_future(starRead('/www/wwwlogs/local.test.com.log')))

    for i in range(task_num):
        task.append(asyncio.ensure_future(sendLineToSever(_deque)))

    done, pading = await asyncio.wait(task)
    print(pading)

def mainProcess(deque):
    loop = asyncio.get_event_loop()
    # asyncio.run(start(deque))
    # loop.run_forever()
    loop.run_until_complete(start(deque))

def subProcess(_deque):
    loop = asyncio.get_event_loop()
    asyncio.run(subProcessStart(_deque))
    loop.run_forever()

if __name__ == "__main__":

    # logPath = '/www/wwwlogs/local.test.com.log'
    #
    # starRead(logPath)

    _deque = deque()
    queue = Queue()

    mainProcess(_deque)
    print('123')
    # p = Process(target=subProcess ,args=(_deque,))
    # p.start()
    # p.join()
