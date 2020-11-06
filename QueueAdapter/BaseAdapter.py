from abc import abstractmethod,ABCMeta



class Adapter():
    __metaclass__ = ABCMeta


    def __init__(self): pass

    @abstractmethod
    def initQueue(self): pass

    @abstractmethod
    def pushDatatoQueue(self ): pass

    @abstractmethod
    def getDataFromQueue(self): pass

    @abstractmethod
    def getDataCountNum(self): pass