from multiprocessing.managers import BaseManager,ListProxy
from multiprocessing import Queue
from multiprocessing.connection import Listener
import time



class MyManager(BaseManager):
    pass




m = MyManager(address=('0.0.0.0',5656),authkey=b'123123')


# subprocess start -------------------begin

# queue = Queue()
# task = []
#
# m.register('get_queue',lambda :queue)
# m.register('get_task',lambda :task ,ListProxy)
#
# m.start()
#
# q = m.get_queue()
# t = m.get_task()
#
# for i in range(10):
#     q.put(i)
#     t.append(i)
#
# while True:
#     time.sleep(1)
#     print('queue len %s ' % q.qsize())
#     print('task len %s' % len(t))

# subprocess start -------------------end


s = m.get_server()
queue = Queue()
task = []

m.register('get_queue',lambda :queue)
m.register('get_task',lambda :task ,ListProxy)

s.serve_forever()




# if __name__ == "__main__":


    # print(s.debug_info())

    # m.start()