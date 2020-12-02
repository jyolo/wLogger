# coding=UTF-8
from Src.Core import OutputCustomer,Reader,Base
from multiprocessing import Manager,Process,Pool,Pipe,Value
from threading import Thread
from webServer.start import start_web
import multiprocessing,time,sys,os,click




def runReader(log_files_conf):


    r = Reader(log_file_conf=log_files_conf)

    pushQueue = ['pushDataToQueue'] * multiprocessing.cpu_count()
    jobs = ['readLog','cutFile'] + pushQueue

    t = []
    for i in jobs:
        th = Thread(target=r.runMethod, args=(i,))
        t.append(th)

    for i in t:
        i.start()

    for i in t:
        i.join()

def customer(  ):
    OutputCustomer().saveToStorage()

def getLogFilsDict(base):
    logFiles = []

    for i in list(base.conf):
        if 'inputer.log_file' in i:
            item = dict(base.conf[i])
            item['app_name'] = i.split('.')[-1]
            logFiles.append(item)

    return logFiles


@click.command()
@click.option('-r', '--run', help="run type" ,type=click.Choice(['inputer', 'outputer','web']))
@click.option('-s', '--stop', help="stop the proccess" ,type=click.Choice(['inputer', 'outputer']))
@click.option('-c', '--config', help="config file name" )
def enter(run,stop,config):

    base = Base(config_name=config)

    if (run == 'inputer'):

        logFiles = getLogFilsDict(base)

        plist = []
        for i in logFiles:
            p = Process(target=runReader, args=( i, ))
            plist.append(p)

        for i in plist:
            i.start()

        for i in plist:
            i.join()

    if (run == 'outputer'):
        p_list = []
        for i in range(int(base.conf['outputer']['worker_process_num'])):
            p = Process(target=customer )
            p_list.append(p)

        for i in p_list:
            i.start()

        for i in p_list:
            i.join()

    if (run == 'web'):
        web_conf = dict(base.conf['web'])
        web_conf[ web_conf['data_engine'] ] = dict(base.conf[ web_conf['data_engine'] ])
        start_web(web_conf)

    if (stop ):
        res = os.popen('ps -ax | grep "main.py -r %s"' % stop)
        pids = []
        print('|============================================================')
        for i in res.readlines():
            if i.find('grep') != -1:
                continue
            print('| %s ' % i.strip())
            pids.append(i.strip().split(' ')[0])


        if len(pids) == 0:
            print('| %s is not running ' % stop)
            print('|============================================================')
            exit('nothing happened . bye bye !')


        print('|============================================================')

        confirm = input('confirm: please enter [ yes | y ] or [ no | n ]  : ')

        if confirm in ['yes','y'] and len(pids) > 0:

            os.popen('kill %s' % ' '.join(pids))
            exit('pid: %s was killed and %s is stoped. bye bye !' % (' '.join(pids) ,stop) )
        else:
            exit('nothing happened . bye bye !')




if __name__ == "__main__":

    enter()
