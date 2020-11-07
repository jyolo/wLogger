# coding=UTF-8
from src.core import OutputCustomer,Reader,Base
from multiprocessing import Manager,Process,Pool,Pipe
from threading import Thread
from webServer.start import start_web
import multiprocessing,time,sys,os



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

def customer(multi_queue = None ,share_list = None ):
    print('subproccess pid  : %s ---------- start' % os.getpid())
    obj = OutputCustomer(multi_queue = multi_queue )
    obj.share_list = share_list
    obj.saveToStorage()


def getLogFilsDict(conf):
    logFiles = []

    for i in list(conf):
        if 'inputer.log_file' in i:
            item = dict(base.conf[i])
            item['app_name'] = i.split('.')[-1]
            logFiles.append(item)

    return logFiles



if __name__ == "__main__":

    base = Base()

    args = sys.argv[1:]
    if args[0] == '-run' :
        if args[1] == 'inputer':

            logFiles = getLogFilsDict(base.conf)

            plist = []
            for i in logFiles:
                p = Process(target=runReader, args=( i, ))
                plist.append(p)

            for i in plist:
                i.start()

            for i in plist:
                i.join()

        elif args[1] == 'outputer':

            if base.conf['outputer']['queue'] == 'mongodb':

                subprocces_error_flag = False
                def _error_done(e):
                    global subprocces_error_flag
                    print('子进程错误: %s' % e)
                    subprocces_error_flag = True

                manager = Manager()

                multi_queue = manager.Queue()
                multi_share_list = manager.list()


                obj = OutputCustomer(multi_queue)

                pnum = int(base.conf['outputer']['worker_process_num'])

                p = Pool(pnum)
                result = []
                for i in range(pnum):
                    r = p.apply_async(customer , args=(multi_queue , multi_share_list ,) , error_callback= _error_done,)
                    result.append(r)
                    multi_queue.put(i) # 标识

                takenum = int(base.conf['outputer']['worker_process_num']) * int(base.conf['outputer']['max_batch_insert_db_size'])
                current_page = 0
                first_send = 0
                while True:
                    time.sleep(1)

                    if subprocces_error_flag == True:
                        break

                    # if (multi_queue.qsize() != int(base.conf['outputer']['worker_process_num'])):
                    #     continue

                    print(multi_queue.qsize())

                    # 有数据没有处理完 等待
                    unhandle_data_num = obj.queue_handle.db[obj.inputer_queue_key].count_documents({'out_queue':0})
                    if multi_queue.qsize() == 0 or (unhandle_data_num > 0 and first_send == 0):
                        first_send = 1

                        page = current_page * takenum
                        start_time = time.perf_counter()

                        res = obj.queue_handle.db[obj.inputer_queue_key].find({'out_queue':0},projection={'data':1}).sort([('add_time',-1)]).limit(takenum)
                        # res = obj.queue_handle.db[obj.inputer_queue_key].find({'out_queue':0},projection={'data':1}).sort([('add_time',-1)]).skip(page).limit(takenum)
                        end_time = time.perf_counter()
                        print('master pid: %s 查询数据: %s 耗时: %s ' % (os.getpid(), len(multi_share_list), end_time - start_time))
                        if res:
                            for i in res:
                                multi_share_list.append(i)
                            # for i in res:
                            #     multi_queue.put(i)

                        end_time = time.perf_counter()
                        print('master pid: %s---遍历数据: %s 耗时: %s ' % (os.getpid(), len(multi_share_list), end_time - start_time))

                        # for i in range(pnum):
                        #     print(pnum)
                        #     multi_queue.put(i)

                p.close()
                p.join()



                # takenum = int(base.conf['outputer']['worker_process_num']) * int(base.conf['outputer']['max_batch_insert_db_size'])
                # current_page = 0
                #
                #
                # while True:
                #     offset = current_page * takenum
                #     start_time = time.perf_counter()
                #     res = obj.queue_handle.db[obj.inputer_queue_key].find().sort([('add_time',-1)]).skip(offset).limit(takenum)
                #
                #     for i in res:
                #         multi_queue.put(i)
                #
                #     end_time = time.perf_counter()
                #     # print(multi_queue.qsize())
                #
                #     print('取数据 耗时: %s ' % (end_time - start_time))
                #
                #     p_list = []
                #     for i in range(int(base.conf['outputer']['worker_process_num'])):
                #         p = Process(target=customer ,args = (multi_queue,) )
                #         p_list.append(p)
                #
                #     for i in p_list:
                #         i.start()
                #
                #     for i in p_list:
                #         i.join()




            elif base.conf['outputer']['queue'] == 'redis':
                p_list = []
                for i in range( int(base.conf['outputer']['worker_process_num']) ):
                    p = Process(target = customer)
                    p_list.append(p)

                for i in p_list:
                    i.start()

                for i in p_list:
                    i.join()

        elif args[1] == 'web':
            web_conf = dict(base.conf['web'])
            web_conf[ web_conf['data_engine'] ] = dict(base.conf[ web_conf['data_engine'] ])

            start_web(web_conf)


        else:
            raise ValueError('example: python3 main.py -run [inputer | outputer]')

    elif args[0] == '-stop':
        if args[1] not in ['inputer' ,'outputer']:
            raise ValueError('-stop only support [inputer | outputer]')


        res = os.popen('ps -ax | grep "main.py -run %s"' % args[1])
        pids = []
        print('|============================================================')
        for i in res.readlines():
            if i.find('grep') != -1:
                continue
            print('| %s ' % i.strip())
            pids.append(i.strip().split(' ')[0])


        if len(pids) == 0:
            print('| %s is not running ' % args[1])
            print('|============================================================')
            exit('nothing happened . bye bye !')


        print('|============================================================')

        confirm = input('confirm: please enter [ yes | y ] or [ no | n ]  : ')

        if confirm in ['yes','y'] and len(pids) > 0:

            os.popen('kill %s' % ' '.join(pids))
            exit('pid: %s was killed and %s is stoped. bye bye !' % (' '.join(pids) ,args[1]) )
        else:
            exit('nothing happened . bye bye !')

    else:
        raise ValueError('example: python3 run.py -run [inputer | outputer]')