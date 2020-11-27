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
    field_map = None
    key_field_map = None


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
                    self.logging.debug(
                        '\n outputerer ---pid: %s wait for queue data \n ' %
                        (os.getpid()))
                    continue

                start_time = time.perf_counter()

                # 　错误退回队列 (未解析的原始的数据)
                self.backup_for_push_back_queue = []

                _data = []
                for item in queue_data:
                    if isinstance(item, bytes):
                        item = item.decode(encoding='utf-8')

                    self.backup_for_push_back_queue.append(item)
                    # 解析日志数据
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

            # 解析完成 批量入库
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

    # 根据数据 和 nginx 解析器中的format 创建 mysql 字段类型的映射
    def build_field_map(self,example_data):
        field_map = {}
        key_field_map = {}

        # 开始组装 mysql字典
        data_key = list(example_data.keys())
        for i in self.runner.logParse.format:
            format_key = i.replace('$', '')

            # 检查默认值是否在数据中
            if format_key in data_key:

                if 'mysql_key_field' in self.runner.logParse.format[i]:
                    key_field_map[format_key] = self.runner.logParse.format[i]['mysql_key_field']

                if 'mysql_field_type' in self.runner.logParse.format[i]:
                    field_map[format_key] = self.runner.logParse.format[i]['mysql_field_type']
                else:
                    field_map[format_key] = 'varchar(255)'

            # 检查 nickname 是否在数据中
            elif 'nickname' in self.runner.logParse.format[i] \
                    and self.runner.logParse.format[i]['nickname'] in data_key:

                if 'mysql_key_field' in self.runner.logParse.format[i]:
                    key_field_map[ self.runner.logParse.format[i]['nickname'] ] = self.runner.logParse.format[i]['mysql_key_field']

                if 'mysql_field_type' in self.runner.logParse.format[i]:
                    field_map[ self.runner.logParse.format[i]['nickname'] ] = self.runner.logParse.format[i]['mysql_field_type']
                else:
                    field_map[ self.runner.logParse.format[i]['nickname'] ] = 'varchar(255)'


            # 检查 extend_field 是否在数据中
            if 'extend_field' in self.runner.logParse.format[i]:
                _intersection = set(data_key).intersection(
                    set(list(self.runner.logParse.format[i]['extend_field'].keys())))

                if len(_intersection):
                    for k in _intersection:

                        if 'mysql_key_field' in  self.runner.logParse.format[i]['extend_field'][k]:
                            key_field_map[k] =  self.runner.logParse.format[i]['extend_field'][k]['mysql_key_field']

                        if 'mysql_field_type' in self.runner.logParse.format[i]['extend_field'][k]:
                            field_map[k] = self.runner.logParse.format[i]['extend_field'][k]['mysql_field_type']
                        else:
                            field_map[k] = 'varchar(255)'


        return field_map , key_field_map

    def __insertToMysql(self,data):

        if self.field_map == None:
            self.field_map ,self.key_field_map = self.build_field_map(data[0])


        fields = None


        _valuelist = []
        for item in data:

            fk = list(item.keys())
            fields = ','.join(fk)


            for i in item:

                field_type = self.field_map[i]

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


    def getKeyFieldStrForCreateTableFromList(self,key_field_needed ,i):

        def func(vars,i):
            _list = []

            if vars.find('.') > -1:
                _key = vars.split('.')
                if _key[1] in self.runner.logParse.format[_key[0]]['extend_field']:
                    _list.append('KEY `{0}_{1}` (`{0}`,`{1}`)'.format(i, _key[1]))
            else:
                if 'nickname' in self.runner.logParse.format[vars]:
                    field_name = self.runner.logParse.format[vars]['nickname']
                else:
                    field_name = vars.replace('$', '')

                _list.append('KEY `{0}_{1}` (`{0}`,`{1}`)'.format(i, field_name))

            return _list

        karg = []

        for args in key_field_needed[i]:

            if isinstance(args, str):

                karg = karg + func(args,i)

            elif isinstance(args, list):

                for g in args:

                    karg = karg + func(g, i)


        return karg

    def __createTable(self,org_data):

        if len(org_data) > 0:


            fields = []
            for i in self.field_map:
                _str = "`%s` %s NOT NULL " % (i ,self.field_map[i] )
                fields.append(_str)


            # 从字段中获取需要创建索引的 字段
            key_field_needed = self.key_field_map

            key_str = ''
            if len(key_field_needed):
                karg = []
                for i in key_field_needed:

                    if isinstance(key_field_needed[i],str): # 字符串
                        karg.append('{0} `{1}` (`{1}`)'.format(key_field_needed[i].upper() ,i))
                    elif isinstance(key_field_needed[i],bool):
                        karg.append('KEY `{0}` (`{0}`)'.format(i))
                    elif isinstance(key_field_needed[i], list):
                        karg.append('KEY `{0}` (`{0}`)'.format(i))
                        karg = karg + self.getKeyFieldStrForCreateTableFromList(key_field_needed,i)


                # 去重
                karg = list(set(karg))

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





