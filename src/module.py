from multiprocessing.managers import BaseManager,ListProxy
from multiprocessing import Queue
from collections import deque
from pymongo import MongoClient
from redis import Redis
import time,json

redis = Redis(host='127.0.0.1',port=6379 ,db=1)

mongo = MongoClient(host='127.0.0.1',port=27017)


class MyManager(BaseManager):

    def __init__(self,*args,**kwargs):
        super(MyManager,self).__init__(*args,**kwargs)





m = MyManager(address=('0.0.0.0',5656),authkey=b'123123')



if __name__ == "__main__":

    # subprocess start -------------------begin


    queue = Queue()
    _deque = deque()


    task = []

    m.register('get_deque',lambda :_deque ,ListProxy)
    m.register('get_queue',lambda :queue )


    # m.register('get_task',lambda :task ,ListProxy)

    m.start()

    dq = m.get_deque()
    q = m.get_queue()





    for i in range(10):
        dq.append(i)

    pipe = redis.pipeline(transaction=True)


    while True:
        time.sleep(1)
        start_time = time.time()
        print('start_time : %s' % start_time)

        # print('dqueue len : %s ;queue len : %s' % (len(dq) ,q.qsize()))
        # print(dq.pop())

        ever_take_num = 100000

        total = redis.llen('log_line')
        lines_list = []
        if(total):
            if total < ever_take_num:
                take_num = total
            else:
                take_num = ever_take_num

            for i in range(take_num):
                pipe.rpop('log_line')

            lines = pipe.execute()

            for item in lines:
                lines_list.append(json.loads(lines[0].decode(encoding='utf-8')))

        if len(lines_list):
            print(len(lines_list))
            a = mongo['test']['line'].insert_many(lines_list,ordered=False,bypass_document_validation=True)
            # print(a.inserted_ids)

            del lines_list
            end_time = time.time()
            print('耗时: %s' % (end_time - start_time))


        # print('queue len %s ' % q.__len__())
        # print('task len %s' % len(t))


    # subprocess start -------------------end