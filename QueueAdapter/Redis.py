from QueueAdapter.BaseAdapter import Adapter
from redis import Redis,exceptions as redis_exceptions
import time,threading,os,json


class QueueAp(Adapter):

    db = None
    runner = None


    @classmethod
    def initQueue(cls,runnerObject):
        self = cls()
        self.runner = runnerObject
        self.conf = self.runner.conf
        self.logging = self.runner.logging

        try:

            self.db = Redis(
                host=self.conf['redis']['host'],
                port=int(self.conf['redis']['port']),
                password=str(self.conf['redis']['password']),
                db=self.conf['redis']['db'],
            )


        except redis_exceptions.RedisError as e:
            self.runner.event['stop'] = e.args[0]


        return self


    def pushDataToQueue(self ):

        pipe = self.db.pipeline()

        retry_reconnect_time = 0

        while True:
            time.sleep(1)
            if self.runner.event['stop']:
                self.logging.error( '%s ; pushQueue threading stop pid: %s ---- tid: %s ' % (self.runner.event['stop'] ,os.getpid() ,threading.get_ident() ))
                return

            try:

                # 重试连接queue的时候; 不再从 dqueue 中拿数据
                if retry_reconnect_time == 0:

                    start_time = time.perf_counter()
                    # self.logging.debug("\n pushQueue -------pid: %s -tid: %s-  started \n" % ( os.getpid(), threading.get_ident()))

                    for i in range(self.runner.max_batch_push_queue_size):
                        try:
                            line = self.runner.dqueue.pop()
                        except IndexError as e:
                            # self.logging.info("\n pushQueue -------pid: %s -tid: %s- wait for data ;queue len: %s---- start \n" % ( os.getpid(), threading.get_ident(), len(list(self.dqueue))))
                            break

                        data = {}
                        data['node_id'] = self.runner.node_id
                        data['app_name'] = self.runner.app_name
                        data['log_format_name'] = self.runner.log_format_name

                        data['line'] = line.strip()

                        try:
                            # 日志的原始字符串
                            data['log_format_str'] = self.runner.server_conf[self.runner.log_format_name]['log_format_str'].strip()
                            # 日志中提取出的日志变量
                            data['log_format_vars'] = self.runner.server_conf[self.runner.log_format_name]['log_format_vars'].strip()

                        except KeyError as e:
                            self.runner.event['stop'] = self.runner.log_format_name + '日志格式不存在'
                            break


                        data = json.dumps(data)
                        pipe.lpush(self.runner.queue_key, data)


                res = pipe.execute()
                if len(res):
                    retry_reconnect_time = 0
                    end_time = time.perf_counter()
                    self.logging.debug("\n pushQueue -------pid: %s -tid: %s- push data to queue :%s ; queue_len : %s----耗时:%s \n"
                          % (os.getpid(), threading.get_ident(), len(res),self.db.llen(self.runner.queue_key), round(end_time - start_time, 2)))

                    
            except redis_exceptions.RedisError as e:


                retry_reconnect_time = retry_reconnect_time + 1

                if retry_reconnect_time >= self.runner.max_retry_reconnect_time :
                    self.runner.event['stop'] = 'pushQueue 重试连接 queue 超出最大次数'
                else:
                    time.sleep(2)
                    self.logging.debug('pushQueue -------pid: %s -tid: %s-  push data fail; reconnect Queue %s times' % (os.getpid() , threading.get_ident() , retry_reconnect_time))

                continue
        pass

    def getDataFromQueue(self):
        start_time = time.perf_counter()

        pipe = self.db.pipeline()

        db_queue_len = self.db.llen(self.runner.queue_key)

        if db_queue_len >= self.runner.max_batch_insert_db_size:
            num = self.runner.max_batch_insert_db_size
        else:
            num = db_queue_len

        for i in range(num):
            # 后进先出
            pipe.rpop(self.runner.queue_key)

        queue_list = pipe.execute()


        # 过滤掉None
        if queue_list.count(None):
            queue_list = list(filter(None, queue_list))

        end_time = time.perf_counter()
        if len(queue_list):
            self.logging.debug("\n pid: %s ;tid : %s-- take len: %s ; queue db len : %s ; ----end 耗时: %s \n" %
                  (os.getpid(), threading.get_ident(), len(queue_list), self.db.llen(self.runner.queue_key),
                   round(end_time - start_time, 2)))

        return queue_list


    # 退回队列
    def rollBackToQueue(self,data):
        pipe = self.db.pipeline()
        for i in data:
            i = bytes(i,encoding='utf-8')
            pipe.rpush(self.runner.queue_key,i)

        pipe.execute()


    def getDataCountNum(self):
        return self.db.llen(self.runner.queue_key)

