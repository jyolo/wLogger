from src.client import OutputCustomer,Reader,Base
from multiprocessing import Queue,Process
from threading import Thread
from server.start import start_server
import multiprocessing,time,sys,os





def runReader(log_files_conf):


    r = Reader(log_file_conf=log_files_conf)

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


def customer():
    obj = OutputCustomer()
    getattr(obj, obj.call_engine)()

def getLogFilsDict(conf):
    logFiles = []

    for i in list(conf):
        if 'client.log_file' in i:
            item = dict(base.conf[i])
            item['app_name'] = i.split('.')[-1]
            logFiles.append(item)

    return logFiles

if __name__ == "__main__":

    base = Base()

    args = sys.argv[1:]
    if args[0] == '-run' :
        if args[1] == 'client':

            logFiles = getLogFilsDict(base.conf)

            plist = []
            for i in logFiles:
                p = Process(target=runReader, args=( i, ))
                plist.append(p)


            for i in plist:
                i.start()

            for i in plist:
                i.join()

        elif args[1] == 'customer':

            web_conf = dict(base.conf['custom.web'])
            web_p = Process(target = start_server ,args = (web_conf,))

            p_list = []
            for i in range( int(base.conf['custom']['worker_process_num']) ):
                p = Process(target = customer)
                p_list.append(p)

            p_list.append(web_p)

            for i in p_list:
                i.start()

            for i in p_list:
                i.join()


        else:
            raise ValueError('example: python3 startClient.py -run [client | customer]')
    else:
        raise ValueError('example: python3 startClient.py -run [client | customer]')