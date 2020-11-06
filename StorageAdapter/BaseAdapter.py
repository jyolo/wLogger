from abc import abstractmethod,ABCMeta



class Adapter():
    __metaclass__ = ABCMeta


    def __init__(self): pass

    @abstractmethod
    def initStorage(self): pass

    @abstractmethod
    def pushDatatoStorage(self ): pass

