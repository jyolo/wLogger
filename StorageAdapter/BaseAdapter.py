from abc import abstractmethod,ABCMeta
import datetime,time


class Adapter():
    __metaclass__ = ABCMeta

    save_engine = []
    split_save = ['day', 'week', 'month' ,'year']

    def __init__(self): pass


    def _getTableName(self,table_key_name):

        table_suffix = ''
        save_engine_conf = self.conf[self.conf['outputer']['save_engine']]
        try:
            self.table = save_engine_conf[table_key_name]
        except KeyError as e:
            raise Exception('配置错误: %s not exists' % e.args)


        if 'split_save' in save_engine_conf:

            if save_engine_conf['split_save'] not in self.split_save:

                raise Exception('outputer 配置项 split_save 只支持 %s 选项' % ','.join(self.split_save))

            if save_engine_conf['split_save'] == 'day':

                table_suffix = time.strftime( '%Y_%m_%d' , time.localtime())

            elif save_engine_conf['split_save'] == 'week':

                now = datetime.datetime.now()
                this_week_start = now - datetime.timedelta(days=now.weekday())
                this_week_end = now + datetime.timedelta(days=6 - now.weekday())

                table_suffix = datetime.datetime.strftime(this_week_start,'%Y_%m_%d') + datetime.datetime.strftime(this_week_end,'_%d')

            elif save_engine_conf['split_save'] == 'month':

                table_suffix = time.strftime( '%Y_%m' , time.localtime())
            elif save_engine_conf['split_save'] == 'year':

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



