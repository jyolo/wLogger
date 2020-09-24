from src.client import loggerParse,OutputCustomer,Reader,Base
from multiprocessing import Queue,Process
from threading import Thread
import multiprocessing,sys



def runOutputCustomer( ):
    obj = OutputCustomer()

    p_list = []
    for i in range( round(multiprocessing.cpu_count() / 2) ):
        p = Process(target = getattr(obj, obj.call_engine))
        p_list.append(p)

    for i in p_list:
        i.start()

    for i in p_list:
        i.join()

def runReader(share_queue ,log_files_conf):

    r = Reader(log_file_conf=log_files_conf, share_queue=share_queue)

    jobs = ['readLog', 'cutFile']
    t = []
    for i in jobs:
        th = Thread(target=r.runMethod, args=(i,))
        th.setDaemon(True)
        t.append(th)

    for i in t:
        i.start()

    for i in t:
        i.join()


def startReader():
    queue = Queue()
    base = Base()
    logFiles = eval(base.conf['client.input']['log_files'].strip())
    plist = []
    for i in logFiles:
        p = Process(target=runReader, args=(queue, i))
        plist.append(p)

    for i in plist:
        i.start()

    for i in plist:
        i.join()




if __name__ == "__main__":

    # startReader()

    # runOutputCustomer()
    args = sys.argv[1:]
    if args[0] == '-run' :
        if args[1] == 'client':
            startReader()
        elif args[1] == 'customer':
            runOutputCustomer()
        else:
            raise ValueError('example: python3 startClient.py -run [client | customer]')
    else:
        raise ValueError('example: python3 startClient.py -run [client | customer]')