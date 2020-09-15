from multiprocessing import Queue
from multiprocessing.managers import BaseManager
from multiprocessing import Queue
from collections import deque
from redis import Redis,RedisError
import time,asyncio,json


redis = Redis(host='127.0.0.1',db=1)

pipe = redis.pipeline(transaction=True)


queue = Queue()
deque = deque()
# #
# m = BaseManager(address=('127.0.0.1',5656),authkey=b'123123')
# #
# m.connect()
#
# m.register('get_queue')
# m.register('get_deque')
# q = m.get_queue()
# dq = m.get_deque()



async def starRead(log_path):
    postion = 0

    while True:

        await asyncio.sleep(1)

        try:

            redis.ping()

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
                    deque.append(line)

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
                print('耗时: %s' % (end_time - start_time))


        except RedisError:
            # print('------------read current position : %s-------------' % fd.tell())
            print('redis connect error wait for redis-sever ')


each_task_handle_num = 100

async def sendLineToSever():

    while True:
        await asyncio.sleep(1)
        for i in range(each_task_handle_num):
            pipe.lpush(deque.pop())

        pipe.execute()
        pipe.close()
        print("\nsendLineToSever : %s --- %s\n" % (time.time() ,len(list(deque))))



async def start():
    task_num = 2
    task = []
    # for i in  range(task_num):
    #     task.append(asyncio.ensure_future(starRead('/www/wwwlogs/local.test.com.log')))
    task.append(asyncio.ensure_future(starRead('/www/wwwlogs/local.test.com.log')))

    for i in range(task_num):
        task.append(asyncio.ensure_future(sendLineToSever()))


    done,pading = await asyncio.wait(task)



if __name__ == "__main__":

    logPath = '/www/wwwlogs/local.test.com.log'

    # starRead(logPath)

    loop = asyncio.get_event_loop()


    asyncio.run(start())
    loop.run_forever()
