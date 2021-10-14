# coding=UTF-8
import time

from Src.Core import OutputCustomer,Reader,Base
from multiprocessing import Process
from threading import Thread
from webServer.start import start_web
import multiprocessing,os,click




def runReader(log_files_conf,config_name):

    r = Reader(log_file_conf=log_files_conf ,config_name=config_name)

    pushQueue = ['pushDataToQueue'] * multiprocessing.cpu_count()
    jobs = ['readLog','cutFile'] + pushQueue

    t = []
    for i in jobs:
        th = Thread(target=r.runMethod, args=(i, ))
        t.append(th)

    for i in t:
        i.start()

    for i in t:
        i.join()

def customer( config_name ):

    OutputCustomer(config_name).saveToStorage()

def analysis(confg_name):

    OutputCustomer(confg_name).watchTraffic()

def getLogFilsDict(base):
    logFiles = []

    for i in list(base.conf):
        if 'inputer.log_file' in i:
            item = dict(base.conf[i])
            item['app_name'] = i.split('.')[-1]
            logFiles.append(item)

    return logFiles


def startInputer(base , config):

    logFiles = getLogFilsDict(base)

    plist = []
    for i in logFiles:
        p = Process(target=runReader, args=(i, config,))
        plist.append(p)

    for i in plist:
        i.start()

    for i in plist:
        i.join()

def startOutputer(base , config):
    p_list = []
    for start_webi in range(int(base.conf['outputer']['worker_process_num'])):
        p = Process(target=customer, args=(config,))
        p_list.append(p)

    for i in p_list:
        i.start()

    for i in p_list:
        i.join()

@click.command()
@click.option('-r', '--run', help="run type" ,type=click.Choice(['inputer', 'outputer','traffic','web']))
@click.option('-s', '--stop', help="stop the proccess" ,type=click.Choice(['inputer', 'outputer']))
@click.option('-c', '--config', help="config file name" )
def enter(run,stop,config):

    if (config == None):
        print('please use "-c" to bind config.ini file')
        exit()

    base = Base(config_name=config)

    if (run == 'inputer'):

        pid = os.fork()
        if pid > 0 :
            exit()
        else:
            startInputer(base, config)



    if (run == 'outputer'):
        # pid = os.fork()
        # if pid > 0:
        #     exit()
        # else:
        #     startOutputer(base, config)
        startOutputer(base, config)


    if (run == 'traffic'):

        analysis(config)

    if (run == 'web'):
        web_conf = dict(base.conf['web'])
        web_conf[ web_conf['data_engine'] ] = dict(base.conf[ web_conf['data_engine'] ])
        start_web(web_conf)

    if (stop ):
        if isinstance(config,str) and len(config) :
            cmd = 'ps -ax | grep "main.py -r %s -c %s"' % (stop, config)
        else:
            cmd = 'ps -ax | grep "main.py -r %s"' % stop

        res = os.popen(cmd)
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
