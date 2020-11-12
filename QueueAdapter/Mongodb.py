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
        self.logging = self.runner.logging

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
        self.db[self.runner.queue_key].create_index([("add_time", -1)], background=True)

        # self.db['asdasd']

        return self


    def pushDataToQueue(self ):

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            if self.runner.event['stop']:
                self.logging.error('%s ; pushQueue threading stop pid: %s ---- tid: %s ' % ( self.runner.event['stop'], os.getpid(), threading.get_ident()))
                return

            try:

                # 重试连接queue的时候; 不再从 dqueue 中拿数据
                if retry_reconnect_time == 0:
                    _queuedata = []

                    start_time = time.perf_counter()

                    for i in range(self.runner.max_batch_push_queue_size):
                        try:
                            line = self.runner.dqueue.pop()
                        except IndexError as e:
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


                if len(_queuedata):
                    res = self.db[self.runner.queue_key].insert_many(_queuedata, ordered=False)

                    end_time = time.perf_counter()
                    self.logging.debug(
                        "\n pushQueue -------pid: %s -tid: %s- push data to queue :%s ; queue_len : %s----耗时:%s \n"
                        % (os.getpid(), threading.get_ident(), len(res.inserted_ids), 0,
                           round(end_time - start_time, 2)))


            except pyerrors.PyMongoError as e:

                retry_reconnect_time = retry_reconnect_time + 1

                if retry_reconnect_time >= self.runner.max_retry_reconnect_time:
                    self.logging.error('pushQueue 重试连接 queue 超出最大次数')
                    raise Exception('pushQueue 重试连接 queue 超出最大次数')
                else:
                    time.sleep(1)
                    self.logging.debug('pushQueue -------pid: %s -tid: %s-  push data fail: %s ; reconnect Queue %s times' % (
                        os.getpid(), e.args, threading.get_ident(), retry_reconnect_time))

                continue


    def getDataFromQueue(self):
        db_queue_len = self.getDataCountNum()

        if db_queue_len == 0:
            return []

        if db_queue_len >= self.runner.max_batch_insert_db_size:
            takenum = self.runner.max_batch_insert_db_size
        else:
            takenum = db_queue_len


        start_time = time.perf_counter()

        _data = []
        for i in range(takenum):
            res = self.db[self.runner.queue_key].find_and_modify(
                query={'out_queue': 0},
                update={'$set': {'out_queue': 1}, '$currentDate': {'ttl': True}},
                sort=[('add_time', -1)]
            )

            if res:
                _data.append(res['data'])

        end_time = time.perf_counter()
        self.logging.debug('\n pid: %s  take data from queue %s ,queue db len : %s ;.耗时: %s \n' % (
        os.getpid(), len(_data),self.getDataCountNum(), round(end_time - start_time ,3)))

        return _data




    def getDataCountNum(self):
        return self.db[self.runner.queue_key].count_documents({'out_queue':0})
