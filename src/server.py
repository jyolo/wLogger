from multiprocessing.connection import Listener,answer_challenge,deliver_challenge
from multiprocessing import Process
from threading import Thread
import time


auth_key = b'123123'
task_connect = {}

def accept_handle(server):

    while True:
        with server.accept() as connect:
            print('--------------%s %s is connected server---------' % (server.last_accepted[0], server.last_accepted[1]))

            # t  = Thread(target=requset_handle ,args= (connect , server.last_accepted ,) )
            # t.daemon = True
            # t.start()
            # t.join()

            p = Process(target=requset_handle ,args= (connect ,  server.last_accepted , ))
            p.start()
            p.join()

def requset_handle(connect,client_info):
    print(client_info)
    connect.send('ping')
    while True:
        try:
            data = connect.recv()
            if data == 'alive':
                print("%s %s %s" % (client_info[0] ,client_info[1] ,data)  )
                connect.send('ping')
        except EOFError:
            connect.close()
            print('close client')


with Listener(address=('127.0.0.1',5656) ,authkey=auth_key) as server:


    # t1 = Thread(target=accept_handle ,args=(server,))
    # t1.daemon = True
    # t1.start()
    # t1.join()

    p = Process(target=accept_handle, args=(server,))
    p.start()
    p.join()






    # while True:
    #
    #     print('---------wait for client---------')
    #     with server.accept() as connect:
    #         print('--------------%s %s is connected server---------' % (server.last_accepted[0], server.last_accepted[1]))
    #         task_connect[server.last_accepted[1]] = server.last_accepted[0]
    #         connect.send('ping')
    #
    #         while True:
    #
    #             try:
    #
    #                 time.sleep(1)
    #                 data = connect.recv()
    #                 if data == 'alive':
    #                     print(task_connect)
    #                     connect.send('ping')
    #
    #
    #
    #             except EOFError:
    #                 connect.close()
    #                 del task_connect[server.last_accepted[1]]
    #                 print(task_connect)
    #
    #







