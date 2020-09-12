from abc import abstractmethod,ABCMeta


class Adapter(metaclass=ABCMeta):


    def __init__(self,log_path):
        self.log_path = log_path
        print('base: %s' % log_path)

    @abstractmethod
    def getLogFormat(self):pass

