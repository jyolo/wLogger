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
        'request_body': 'mediumtext',
        'request_length': 'int(10)',
        'request_time': 'float(10,3)',
        'upstream_response_time': 'varchar(255)',
        'status' : 'int(4)',
        'body_bytes_sent': 'int(10)',
        'bytes_sent': 'int(10)',
        'connection': 'int(10)',
        'connection_requests': 'int(10)',
        'http_referer': 'varchar(255)',
        'http_user_agent': 'text',
        'time_local': 'varchar(255)',
        'upstream_addr': 'varchar(255)',
        # 预设附加字段
        'time_str': 'datetime',
        'timestamp': 'int(10)',
        'args' : 'mediumtext', # log_format 中有request　则会有此字段
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

                # reset retry_reconnect_time
                retry_reconnect_time = 0

                end_time = time.perf_counter()
                self.logging.debug("\n outputerer -------pid: %s -- insert into mysql : %s---- end 耗时: %s \n" % (
                    os.getpid(), affected_rows, round(end_time - start_time, 3)))

            except pymysql.err.DataError as e:
                error_msg = "\n outputerer -------pid: %s -- pymysql.err.DataError 数据类型错误 请检查 field_map 配置---- Exceptions: %s \n" % (
                        os.getpid(), e.args)
                self.logging.error( error_msg )
                raise Exception( error_msg)

            except pymysql.err.MySQLError as e:
                time.sleep(2)
                retry_reconnect_time = retry_reconnect_time + 1
                if retry_reconnect_time >= max_retry_reconnect_time:
                    self.runner.rollBackQueue(self.backup_for_push_back_queue)
                    self.logging.error('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                    raise Exception('重试重新链接 mongodb 超出最大次数 %s' % max_retry_reconnect_time)
                else:

                    self.logging.error("\n outputerer -------pid: %s -- retry_reconnect_mysql at: %s time---- Exceptions %s ; %s \n" % (
                        os.getpid(),retry_reconnect_time,e.__class__  ,e.args))
                    continue


    def __insertToMysql(self,data):


        fields = None

        field_map_keys = list(self.field_map)
        field_map_value = list(self.field_map.values())
        _valuelist = []
        for item in data:


            fk = list(item.keys())
            fields = ','.join(fk)

            for i in item:
                field_type = field_map_value[ field_map_keys.index(i) ]
                if (field_type.find('int') > -1 or field_type.find('float') > -1) and str(item[i]) == '':
                    item[i] = '"0"'
                elif str(item[i]) == '' :
                    item[i] = '"-"'
                else:
                    item[i] = '"%s"' % str(item[i]).strip('"')

            values = '(%s)' % ','.join(list(item.values()))
            _valuelist.append(values)


        sql = "INSERT INTO %s(%s)  VALUES %s" % (self.table,fields,','.join(_valuelist))

        self.debug_sql = sql

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

                self.runner.logging.error('Exception: %s ; %s 数据写入错误: %s ;sql: %s' % (e.__class__,e.args , self.debug_sql))
                raise Exception(' Exception: %s ;  数据写入错误: %s ;sql: %s' % (e.__class__,e.args , self.debug_sql))


    def __createTable(self,org_data):

        if len(org_data) > 0:
            line = json.loads(org_data[0])
            reg = re.compile('\$(\w+)?')
            match = reg.findall(line['log_format_str'])
            if len(match):
                # 该三项配置会被转换会 其它 字段
                if 'request' in match:
                    del match[ match.index('request') ]
                if 'time_local' in match:
                    del match[match.index('time_local')]
                if 'time_iso8601' in match:
                    del match[match.index('time_iso8601')]


                match = self.pre_field + match

                fields = []
                for i in match:
                    _str = "`%s` %s NULL " % (i ,self.field_map[i] )
                    fields.append(_str)

                key_field = ['request_url','remote_addr','timestamp','time_str','http_user_agent']
                # 从字段中获取需要创建索引的 字段
                key_field_needed = list(set(match).intersection(set(key_field)))
                key_str = ''
                if len(key_field_needed):
                    karg = []
                    for i in key_field_needed:
                        if i == 'http_user_agent': # ua 全文索引
                            karg.append('FULLTEXT `%s` (`%s`)' % (i, i))
                        else:
                            karg.append('KEY `%s` (`%s`)' % (i,i))

                    key_str = ',' + ','.join(karg)


                sql = """
                        CREATE TABLE IF NOT EXISTS  `%s`.`%s`  (
                                          `id` int(11) NOT NULL AUTO_INCREMENT,
                                          %s ,
                                          PRIMARY KEY (`id`)
                                          %s
                                        )
                                """ % (self.conf['mysql']['db'], self.table ,','.join(fields),key_str)



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





