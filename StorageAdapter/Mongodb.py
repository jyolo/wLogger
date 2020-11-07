from StorageAdapter.BaseAdapter import Adapter
from pymongo import MongoClient,errors as pyerrors
import time,threading,os

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

class StorageAp(Adapter):

    db = None
    runner = None

    @classmethod
    def initStorage(cls,runnerObject):
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

        return self


    def pushDataToStorage(self):


        mongodb_outputer_client = self.db[ self.runner.save_engine_conf['collection'] ]


        if 'max_retry_reconnect_time' in self.conf['outputer']:
            max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
        else:
            max_retry_reconnect_time = 3

        retry_reconnect_time = 0


        while True:
            time.sleep(1)

            # 重试链接的时候 不再 从队列中取数据
            if retry_reconnect_time == 0:
                num = self.runner.queue_handle.getDataCountNum()

                if num == 0:
                    # print('pid: %s wait for data' % os.getpid())
                    continue

                queue_list = self.runner.getQueueData()

                if not queue_list or len(queue_list) == 0:
                    print('subproccess pid: %s wait for data' % os.getpid())
                    continue

                start_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- reg data len: %s---- start \n" % (
                    os.getpid(), len(queue_list)))

                backup_for_push_back_queue = []
                insertList = []
                for i in queue_list:
                    if not i:
                        continue

                    if isinstance(i,bytes):
                        line = i.decode(encoding='utf-8')
                    else:
                        line = i

                    backup_for_push_back_queue.append(line)

                    line_data = self.runner.parse_line_data(line)

                    insertList.append(line_data)

                end_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- reg datas len: %s---- end 耗时: %s \n" % (
                    os.getpid(), len(insertList), round(end_time - start_time, 2)))

            if len(insertList):
                try:
                    start_time = time.perf_counter()
                    # print("\n outputerer -------pid: %s -- insert into mongodb: %s---- start \n" % (
                    # os.getpid(), len(insertList)))

                    # before_into_storage
                    self._handle_queue_data_before_into_storage()

                    res = mongodb_outputer_client.insert_many(insertList, ordered=False)

                    # after_into_storage
                    self._handle_queue_data_after_into_storage()

                    retry_reconnect_time = 0

                    end_time = time.perf_counter()
                    print("\n outputerer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (
                    os.getpid(), len(res.inserted_ids), round(end_time - start_time, 2)))

                except pyerrors.PyMongoError as e:
                    print(e.args)
                    time.sleep(1)
                    retry_reconnect_time = retry_reconnect_time + 1
                    if retry_reconnect_time >= max_retry_reconnect_time:
                        self.runner.push_back_to_queue(backup_for_push_back_queue)
                        raise pyerrors.PyMongoError('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                    else:
                        print("\n outputerer -------pid: %s -- retry_reconnect_mongodb at: %s time---- \n" % (
                        os.getpid(), retry_reconnect_time))
                        continue


    # 在持久化存储之前 对 队列中的数据 进行预处理 ,比如 update ,delete 等操作
    def _handle_queue_data_before_into_storage(self):pass


    # 在持久化存储之前 对 队列中的数据 进行预处理 ,比如 update ,delete 等操作
    def _handle_queue_data_after_into_storage(self):
        if (hasattr(self.runner, 'queue_data_ids')):
            ids = self.runner.queue_data_ids
            self.db[self.runner.inputer_queue_key].update_many(
                {'_id': {'$in': ids}},
                {
                    '$set': {'out_queue': 1},
                    '$currentDate': {'ttl': True}
                },

            )

            self.runner.multi_queue.get()
