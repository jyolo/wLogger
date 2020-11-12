from abc import abstractmethod,ABCMeta
from src.Adapter import Base


class Adapter(Base):
    __metaclass__ = ABCMeta


    def __init__(self): pass

    @abstractmethod
    def initQueue(self): pass

    @abstractmethod
    def pushDataToQueue(self ): pass

    @abstractmethod
    def getDataFromQueue(self): pass

    @abstractmethod
    def rollBackToQueue(self): pass

    @abstractmethod
    def getDataCountNum(self): pass