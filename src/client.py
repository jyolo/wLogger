from multiprocessing.connection import Client,deliver_challenge,answer_challenge
from multiprocessing import connection
import time

auth_key = b'123123'

# with Client(address=('127.0.0.1',5656) ,authkey=b'123123') as client:
#
#     while True:
#         time.sleep(1)
#         if client.readable:
#             client.send('123')
#             client.close()
#


with Client(address=('127.0.0.1', 5656), authkey=b'123123') as client:
    print(client.fileno())
    print(client.writable)
    print(client.readable)
    while client.readable:
        time.sleep(1)
        print('11111111')
        data = client.recv()
        if data == 'ping':
            print(data)
            client.send('alive')





