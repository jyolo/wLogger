from StorageAdapter.BaseAdapter import Adapter
import time,threading,os,traceback,pymysql


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
        self.db = pymysql.connect(
            host = self.conf['mongodb']['host'],
            port = int(self.conf['mongodb']['port']),
            user = quote_plus(self.conf['mongodb']['username']),
            password = quote_plus(self.conf['mongodb']['password']),
            db = quote_plus(self.conf['mongodb']['db'])
        )


        return self


    def pushDataToStorage(self):
        retry_reconnect_time = 0

        while True:
            time.sleep(0.1)

            if retry_reconnect_time == 0:

                # 获取队列数据
                queue_data = self.runner.getQueueData()
                if len(queue_data) == 0:
                    continue

                start_time = time.perf_counter()

                # 　错误退回队列 (未解析的原始的数据)
                backup_for_push_back_queue = []
                _data = []
                for item in queue_data:
                    if isinstance(item, bytes):
                        item = item.decode(encoding='utf-8')

                    backup_for_push_back_queue.append(item)
                    item = self.runner._parse_line_data(item)
                    _data.append(item)


                end_time = time.perf_counter()

                take_time = round(end_time - start_time, 3)
                print(
                    '\n outputerer ---pid: %s tid: %s reg data len:%s;  take time :  %s' %
                    (os.getpid(), threading.get_ident(), len(_data), take_time))



            if 'max_retry_reconnect_time' in self.conf['outputer']:
                max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
            else:
                max_retry_reconnect_time = 3

            mongodb_outputer_client = self.db[self.runner.save_engine_conf['collection']]
            try:
                start_time = time.perf_counter()

                if len(_data) == 0:
                    continue

                # before_into_storage
                self._handle_queue_data_before_into_storage()

                sql = "INSERT INTO test_mysql (name, num, text) VALUES ('{0}','{1}', '{2}')".format('Zarten_1', 1,'mysql test')
                try:
                    with self.db.cursor() as cursor:
                        cursor.execute(sql)
                    self.db.commit()
                except pymysql.err.MySQLError as e:
                    self.db.rollback()

                # after_into_storage
                self._handle_queue_data_after_into_storage()

                # 重置 retry_reconnect_time
                retry_reconnect_time = 0

                end_time = time.perf_counter()
                print("\n outputerer -------pid: %s -- insert into mongodb: %s---- end 耗时: %s \n" % (
                    os.getpid(), len(res.inserted_ids), round(end_time - start_time, 3)))


            except pyerrors.PyMongoError as e:
                print(e.args)
                time.sleep(1)
                retry_reconnect_time = retry_reconnect_time + 1
                if retry_reconnect_time >= max_retry_reconnect_time:
                    self.runner.rollBackQueue(backup_for_push_back_queue)
                    raise pyerrors.PyMongoError('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                else:
                    print("\n outputerer -------pid: %s -- retry_reconnect_mongodb at: %s time---- \n" % (
                        os.getpid(), retry_reconnect_time))
                    continue


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





