from StorageAdapter.BaseAdapter import Adapter
from pymongo import MongoClient,errors as pyerrors
from threading import Thread
import time,threading,os,traceback


try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

class StorageAp(Adapter):

    db = None
    runner = None
    STOP = False

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


    def _parseData(self):

        retry_reconnect_time = 0

        while True:
            time.sleep(0.1)

            if self.runner.multi_queue.qsize() == 0:
                # print('pid: %s tid: %s _parseData multi_queue len: %s' % (
                # os.getpid(), threading.get_ident(), self.runner.multi_queue.qsize()))
                continue


            if retry_reconnect_time == 0:

                start_time = time.perf_counter()

                _data = []
                for i in range(5000):
                    if self.runner.multi_queue.qsize() == 0 :
                        break

                    item = self.runner.multi_queue.get_bucket()
                    if not item:
                        break

                    if isinstance(item, bytes):
                        item = item.decode(encoding='utf-8')

                    item = self.runner.parse_line_data(item)
                    _data.append(item)


                end_time = time.perf_counter()

                take_time = round(end_time - start_time, 3)
                print('\n outputerer ---pid: %s tid: %s reg data len:%s; dqueue len:%s ;multi_queue len :%s; take time :  %s' %
                    (os.getpid(), threading.get_ident(), len(_data), len(self.runner.dqueue),
                     self.runner.multi_queue.qsize(), take_time))


            if 'max_retry_reconnect_time' in self.conf['outputer']:
                max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
            else:
                max_retry_reconnect_time = 3


            mongodb_outputer_client = self.db[self.runner.save_engine_conf['collection']]
            try:
                start_time = time.perf_counter()

                if len(_data) == 0:
                    continue

                backup_for_push_back_queue = _data
                # before_into_storage
                self._handle_queue_data_before_into_storage()

                res = mongodb_outputer_client.insert_many(_data, ordered=False)

                # after_into_storage
                self._handle_queue_data_after_into_storage()

                retry_reconnect_time = 0

                end_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (
                    os.getpid(), len(res.inserted_ids), round(end_time - start_time, 3)))

                _data = []

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


    def _intoDb(self):

        if 'max_retry_reconnect_time' in self.conf['outputer']:
            max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
        else:
            max_retry_reconnect_time = 3

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            # print('\n outputerer -------pid: %s tid: %s parseQeueuData _intoDb len: %s' % (os.getpid(), threading.get_ident(), len(self.runner.dqueue) ))

            mongodb_outputer_client = self.db[self.runner.save_engine_conf['collection']]
            try:
                start_time = time.perf_counter()

                data = []
                for i in range(len(self.runner.dqueue)):
                    data.append(self.runner.dqueue.pop())

                if len(data) == 0 :
                    continue
                backup_for_push_back_queue = data
                # before_into_storage
                self._handle_queue_data_before_into_storage()

                res = mongodb_outputer_client.insert_many(data, ordered=False)

                # after_into_storage
                self._handle_queue_data_after_into_storage()

                retry_reconnect_time = 0

                end_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (
                os.getpid(), len(res.inserted_ids), round(end_time - start_time, 3)))

                # 消费完 dqueue 里面的数据后 取出标识 标识已完成
                if hasattr(self.runner,'share_worker_list') and self.runner.share_worker_list != None:
                    self.runner.share_worker_list.pop()


            except pyerrors.PyMongoError as e:
                print(list(self.runner.dqueue))
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



    def pushDataToStorage(self):
        self._parseData()


    # 在持久化存储之前 对 队列中的数据 进行预处理 ,比如 update ,delete 等操作
    def _handle_queue_data_before_into_storage(self):
        pass

    # 在持久化存储之前 对 队列中的数据 进行预处理 ,比如 update ,delete 等操作
    def _handle_queue_data_after_into_storage(self):
        # if (hasattr(self.runner, 'queue_data_ids')):
        #     ids = self.runner.queue_data_ids
        #     self.db[self.runner.queue_key].update_many(
        #         {'_id': {'$in': ids}},
        #         {
        #             '$set': {'out_queue': 1},
        #             '$currentDate': {'ttl': True}
        #         },
        #
        #     )
        pass





