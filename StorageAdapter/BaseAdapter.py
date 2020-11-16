from abc import abstractmethod,ABCMeta
import datetime,time


class Adapter():
    __metaclass__ = ABCMeta

    save_engine = []
    save_engine_log_split = ['day', 'week', 'month' ,'year']

    def __init__(self): pass


    def _getTableName(self,table_key_name):

        table_suffix = ''

        try:
            self.table = self.conf[self.conf['outputer']['save_engine']][table_key_name]
        except KeyError as e:
            raise Exception('配置错误: %s not exists' % e.args)


        if 'save_engine_log_split' in self.conf['outputer']:

            if self.conf['outputer']['save_engine_log_split'] not in self.save_engine_log_split:

                raise Exception('outputer 配置项 save_engine_log_split 只支持 %s 选项' % ','.join(self.save_engine_log_split))

            if self.conf['outputer']['save_engine_log_split'] == 'day':

                table_suffix = time.strftime( '%Y_%m_%d' , time.localtime())

            elif self.conf['outputer']['save_engine_log_split'] == 'week':

                now = datetime.datetime.now()
                this_week_start = now - datetime.timedelta(days=now.weekday())
                this_week_end = now + datetime.timedelta(days=6 - now.weekday())

                table_suffix = datetime.datetime.strftime(this_week_start,'%Y_%m_%d') + datetime.datetime.strftime(this_week_end,'_%d')

            elif self.conf['outputer']['save_engine_log_split'] == 'month':

                table_suffix = time.strftime( '%Y_%m' , time.localtime())
            elif self.conf['outputer']['save_engine_log_split'] == 'year':

                table_suffix = time.strftime( '%Y' , time.localtime())

        if len(table_suffix) :
            self.table = self.table +'_' + table_suffix



    @abstractmethod
    def initStorage(self): pass

    @abstractmethod
    def pushDataToStorage(self ): pass

    @abstractmethod
    def _handle_queue_data_before_into_storage(self):pass

    @abstractmethod
    def _handle_queue_data_after_into_storage(self):pass



