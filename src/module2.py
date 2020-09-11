from multiprocessing import Queue
from multiprocessing.managers import BaseManager,ListProxy
from multiprocessing import Queue
import time


queue = Queue()

m = BaseManager(address=('192.168.0.188',5656),authkey=b'123123')

m.connect()


m.register('get_queue')
q = m.get_queue()

m.register('get_task')
task = m.get_task()


while True:
    time.sleep(2)
    
    if(len(task) > 0):
        task.pop()
        print(len(task))

        continue

    else:

        break






# m.shutdown_server()





# if __name__ == "__main__":


    # print(s.debug_info())

    # m.start()