from StorageAdapter.BaseAdapter import Adapter
import time,threading,os,json,pymysql,re



try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

class StorageAp(Adapter):

    db = None
    runner = None
    field_map = {
        'node_id':'varchar(255)',
        'app_name':'varchar(255)',
        'remote_addr' : 'varchar(255)',
        'http_x_forwarded_for' : 'varchar(255)',
        'scheme' : 'varchar(255)',
        'request': 'varchar(255)',
        'request_body': 'text',
        'request_length': 'int(10)',
        'request_time': 'float(10,3)',
        'upstream_response_time': 'varchar(255)',
        'status' : 'int(4)',
        'body_bytes_sent': 'int(10)',
        'bytes_sent': 'int(10)',
        'connection': 'int(10)',
        'connection_requests': 'int(10)',
        'http_referer': 'varchar(255)',
        'http_user_agent': 'varchar(255)',
        'time_local': 'varchar(255)',
        'upstream_addr': 'varchar(255)',
        # 预设附加字段
        'time_str': 'datetime',
        'timestamp': 'int(10)',
        'args' : 'varchar(255)', # log_format 中有request　则会有此字段
        'server_protocol' : 'varchar(255)', # log_format 中有request　则会有此字段
        'request_url' : 'varchar(255)', # log_format 中有request　则会有此字段
        'request_method' : 'varchar(10)', # log_format 中有request　则会有此字段
        "isp": "varchar(255)",
        "city": "varchar(255)",
        "city_id": 'int(10)',
        "province": "varchar(255)",
        "country": "varchar(255)"
    }

    # 预设附加字段
    pre_field = [
        'node_id',
        'app_name',
        'time_str',
        'timestamp',
        'args',
        'server_protocol',
        'request_url',
        'request_method',
        'isp',
        'city',
        'city_id',
        'province',
        'country'
    ]

    @classmethod
    def initStorage(cls,runnerObject):
        self = cls()
        self.runner = runnerObject
        self.conf = self.runner.conf
        self.logging = self.runner.logging
        pymysql_timeout_secends = 60
        try:
            self.db = pymysql.connect(
                host = self.conf['mysql']['host'],
                port = int(self.conf['mysql']['port']),
                user = quote_plus(self.conf['mysql']['username']),
                password = quote_plus(self.conf['mysql']['password']),
                db = quote_plus(self.conf['mysql']['db']),
                connect_timeout = pymysql_timeout_secends,
                read_timeout = pymysql_timeout_secends,
                write_timeout = pymysql_timeout_secends,
            )
        except pymysql.err.MySQLError:
            self.logging.error('Mysql 链接失败,请检查配置文件!')
            raise Exception('Mysql 链接失败,请检查配置文件!')


        return self



    def pushDataToStorage(self):
        retry_reconnect_time = 0

        while True:
            time.sleep(0.1)
            self._getTableName('table')

            if retry_reconnect_time == 0:

                # 获取队列数据
                queue_data = self.runner.getQueueData()
                if len(queue_data) == 0:
                    continue

                start_time = time.perf_counter()

                # 　错误退回队列 (未解析的原始的数据)
                self.backup_for_push_back_queue = []

                _data = []
                for item in queue_data:
                    if isinstance(item, bytes):
                        item = item.decode(encoding='utf-8')

                    self.backup_for_push_back_queue.append(item)

                    item = self.runner._parse_line_data(item)

                    if item:
                        _data.append(item)


                end_time = time.perf_counter()

                take_time = round(end_time - start_time, 3)
                self.logging.debug(
                    '\n outputerer ---pid: %s tid: %s reg data len:%s;  take time :  %s \n ' %
                    (os.getpid(), threading.get_ident(), len(_data), take_time))

            if 'max_retry_reconnect_time' in self.conf['outputer']:
                max_retry_reconnect_time = int(self.conf['outputer']['max_retry_reconnect_time'])
            else:
                max_retry_reconnect_time = 3


            try:
                start_time = time.perf_counter()

                if len(_data) == 0:
                    continue

                # for reconnect
                self.db.ping()

                affected_rows = self.__insertToMysql(_data)

                # after_into_storage
                self._handle_queue_data_after_into_storage()

                # reset retry_reconnect_time
                retry_reconnect_time = 0

                end_time = time.perf_counter()
                self.logging.debug("\n outputerer -------pid: %s -- insert into mysql : %s---- end 耗时: %s \n" % (
                    os.getpid(), affected_rows, round(end_time - start_time, 3)))

            except pymysql.err.MySQLError as e:
                time.sleep(2)
                retry_reconnect_time = retry_reconnect_time + 1
                if retry_reconnect_time >= max_retry_reconnect_time:
                    self.runner.rollBackQueue(self.backup_for_push_back_queue)
                    self.logging.error('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                    raise Exception('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                else:
                    print(_data)
                    self.logging.error("\n outputerer -------pid: %s -- retry_reconnect_mysql at: %s time---- Exceptions: %s \n" % (
                        os.getpid(), retry_reconnect_time ,e.args))
                    continue


    def __insertToMysql(self,data):


        fields = None

        _valuelist = []
        for item in data:
            _values = '('
            for i in item:
                item[i] = '"%s"' % str(item[i]).strip('"')

            if fields == None:
                fk = item.keys()
                fields = ','.join(fk)

            values = '(%s)' % ','.join(item.values())

            _valuelist.append(values)


        sql = "INSERT INTO %s(%s)  VALUES %s" % (self.table,fields,','.join(_valuelist))

        try:
            with self.db.cursor() as cursor:
                affected_rows = cursor.execute(sql)

            self.db.commit()

            return affected_rows
        # when table not found
        except pymysql.err.ProgrammingError as e:
            create_table_flag = self._handle_queue_data_before_into_storage(self.backup_for_push_back_queue)
            # 创建表的时候 补插入数据
            if create_table_flag == True:
                with self.db.cursor() as cursor:
                    affected_rows = cursor.execute(sql)
                self.db.commit()
                return affected_rows
            # 数据表存在的 其它错误
            else:
                self.runner.logging.error(' pymysql.err.ProgrammingError 数据写入错误: %s' % e.args)
                raise Exception(' pymysql.err.ProgrammingError 数据写入错误: %s' % e.args)


    def __createTable(self,org_data):

        if len(org_data) > 0:
            line = json.loads(org_data[0])
            reg = re.compile('\$(\w+)?')
            match = reg.findall(line['log_format_str'])
            if len(match):

                match = self.pre_field + match
                fields = []
                for i in match:
                    _str = "`%s` %s NULL " % (i ,self.field_map[i] )
                    fields.append(_str)

                sql = """
                        CREATE TABLE IF NOT EXISTS  `%s`.`%s`  (
                                          `id` int(11) NOT NULL AUTO_INCREMENT,
                                          %s ,
                                          PRIMARY KEY (`id`)
                                        )
                                """ % (self.conf['mysql']['db'], self.table ,','.join(fields))


                try:
                    with self.db.cursor() as cursor:
                        cursor.execute(sql)
                except pymysql.MySQLError as e:

                    self.logging.error('数据表 %s.%s 创建失败 ;Exception: %s ; SQL:%s' % (self.conf['mysql']['db'], self.conf['mysql']['table'] , e.args ,sql))
                    raise Exception('数据表 %s.%s 创建失败 ;Exception: %s ; SQL:%s' % (self.conf['mysql']['db'], self.conf['mysql']['table'], e.args ,sql))


    # 检查table　是否存在
    def _handle_queue_data_before_into_storage(self ,org_data):

        sql = "SELECT table_name FROM information_schema.TABLES WHERE table_name ='%s'" % self.table;
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchone()
            if not res:
                self.logging.warn('没有发现数据表,开始尝试创建数据表')
                self.__createTable(org_data)
                return True

        return False


    # 在持久化存储之前 对 队列中的数据 进行预处理 ,比如 update ,delete 等操作
    def _handle_queue_data_after_into_storage(self):
        pass





