from QueueAdapter.BaseAdapter import Adapter
from pymongo import MongoClient,errors as pyerrors
import time,threading,os,json

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

class QueueAp(Adapter):

    db = None
    runner = None

    @classmethod
    def initQueue(cls,runnerObject):
        self = cls()
        self.runner = runnerObject
        self.conf = self.runner.conf

        if self.conf['mongodb']['username'] and self.conf['mongodb']['password']:
            mongo_url = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                        (
                            quote_plus(self.conf['mongodb']['username']),
                            quote_plus(self.conf['mongodb']['password']),
                            self.conf['mongodb']['host'],
                            int(self.conf['mongodb']['port']),
                            self.conf['mongodb']['db']
                        )

        else:
            mongo_url = 'mongodb://%s:%s/?authSource=%s' % \
                        (
                            self.conf['mongodb']['host'],
                            int(self.conf['mongodb']['port']),
                            self.conf['mongodb']['db']
                        )

        mongodb = MongoClient(mongo_url)
        self.db = mongodb[self.conf['mongodb']['db']]

        # 创建一个过期索引 过时间
        ttl_seconds = 120
        self.db[self.runner.queue_key].create_index([("ttl", 1)], expireAfterSeconds=ttl_seconds)
        self.db[self.runner.queue_key].create_index([("out_queue", 1)], background=True)

        # self.db['asdasd'].list_indexes()

        return self


    def pushDataToQueue(self ):

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            if self.runner.event['stop']:
                print('%s ; pushQueue threading stop pid: %s ---- tid: %s ' % (
                    self.runner.event['stop'], os.getpid(), threading.get_ident()))
                return

            try:

                # 重试连接queue的时候; 不再从 dqueue 中拿数据
                if retry_reconnect_time == 0:
                    _queuedata = []

                    start_time = time.perf_counter()
                    # print("\n pushQueue -------pid: %s -tid: %s-  started \n" % ( os.getpid(), threading.get_ident()))

                    for i in range(self.runner.max_batch_push_queue_size):
                        try:
                            line = self.runner.dqueue.pop()
                        except IndexError as e:
                            # print("\n pushQueue -------pid: %s -tid: %s- wait for data ;queue len: %s---- start \n" % ( os.getpid(), threading.get_ident(), len(list(self.dqueue))))
                            break

                        q_data = {}
                        data = {}
                        data['node_id'] = self.runner.node_id
                        data['app_name'] = self.runner.app_name
                        data['log_format_name'] = self.runner.log_format_name

                        data['line'] = line.strip()

                        try:
                            data['log_format_str'] = self.runner.server_conf[self.runner.log_format_name].strip()
                        except KeyError as e:
                            self.runner.event['stop'] = self.runner.log_format_name + '日志格式不存在'
                            break

                        data = json.dumps(data)

                        q_data['out_queue'] = 0
                        q_data['add_time'] = time.time()
                        q_data['data'] = data

                        _queuedata.append(q_data)


                        # data['out_queue'] = 0
                        # _queuedata.append(data)

                if len(_queuedata):
                    res = self.db[self.runner.queue_key].insert_many(_queuedata, ordered=False)

                    end_time = time.perf_counter()
                    print(
                        "\n pushQueue -------pid: %s -tid: %s- push data to queue :%s ; queue_len : %s----耗时:%s \n"
                        % (os.getpid(), threading.get_ident(), len(res.inserted_ids), 0,
                           round(end_time - start_time, 2)))


            except pyerrors.PyMongoError as e:

                retry_reconnect_time = retry_reconnect_time + 1

                if retry_reconnect_time >= self.runner.max_retry_reconnect_time:
                    self.runner.event['stop'] = 'pushQueue 重试连接 queue 超出最大次数'
                else:
                    time.sleep(2)
                    print('pushQueue -------pid: %s -tid: %s-  push data fail: %s ; reconnect Queue %s times' % (
                        os.getpid(), e.args, threading.get_ident(), retry_reconnect_time))

                continue


    def getDataFromQueue(self):

        while True:
            time.sleep(1)

            if len(self.runner.share_worker_list) == 0:
                # print('subproccess pid : %s ;tid : %s 等待数据处理中....' % (os.getpid() , threading.get_ident()))
                continue

            start_time = time.perf_counter()
            _data = []
            _ids = []


            for i in range( int(self.conf['outputer']['max_batch_insert_db_size']) ):

                try:
                    res = self.runner.share_list.pop()

                    _ids.append(res['_id'])
                    # self.runner.dqueue.append(res['data'])
                    _data.append(res['data'])

                except Exception:
                    break


            # for 'handle_queue_data_after_into_storage' method
            self.runner.queue_data_ids = _ids
            end_time = time.perf_counter()
            print('subproccess pid : %s take dqueue data : %s  耗时: %s ; share_worker_list : %s' % (os.getpid() , len(_data) , (end_time - start_time) ,len(self.runner.share_worker_list) ))
            del _ids
            return _data


    def getDataCountNum(self):
        return len(self.runner.dqueue)
