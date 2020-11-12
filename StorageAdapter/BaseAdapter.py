from abc import abstractmethod,ABCMeta
from src.Adapter import Base

class Adapter(Base):
    __metaclass__ = ABCMeta


    def __init__(self): pass

    @abstractmethod
    def initStorage(self): pass

    @abstractmethod
    def pushDataToStorage(self ): pass

    @abstractmethod
    def _handle_queue_data_before_into_storage(self):pass

    @abstractmethod
    def _handle_queue_data_after_into_storage(self):pass



