from src.client import loggerParse,Outputer,Reader,Base
from multiprocessing import Queue,Process
from threading import Thread



def startOutput(share_queue  ,log_format_name ):
    obj = Outputer(share_queue=share_queue,log_format_name=log_format_name)
    getattr(obj,obj.call_engine)()



def startReader(share_queue ,log_files_conf):
    r = Reader(log_file_conf=log_files_conf, share_queue=share_queue)
    r.output_process = Process(target=startOutput ,args=(share_queue,log_files_conf['log_format_name'] ) )

    jobs = ['readLog', 'cutFile', 'watcher']
    t = []
    for i in jobs:
        th = Thread(target=r.runMethod, args=(i,))
        th.setDaemon(True)
        t.append(th)

    for i in t:
        i.start()

    for i in t:
        i.join()


def run():
    queue = Queue()
    base = Base()
    logFiles = eval(base.conf['client.input']['log_files'].strip())
    # for i in logFiles:
    #     print(i)

    p = Process(target=startReader ,args=(queue,logFiles[0]))
    p.start()
    p.join()

if __name__ == "__main__":

    run()
