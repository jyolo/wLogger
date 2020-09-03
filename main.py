from configparser import ConfigParser
from multiprocessing import Process,Queue
import os,subprocess,time



class Main:

    def __init__(self):
        self.__root = os.path.dirname(os.path.abspath(__file__))
        self.__config = self.__getConfig()


    def __getConfig(self):
        config_path = self.__root + '/config.ini'

        if ( not os.path.exists(config_path) ):
            raise FileNotFoundError('config.ini not found in the project root path')

        conf = ConfigParser()
        conf.read(config_path, encoding="utf-8")
        print(conf)



    def run(self):
        from server.start import start_server

        p = Process(target=start_server, name='flask_server')
        queue = Queue()

        p.start()

        while True:
            time.sleep(1)
            # print(p.get())
            print(p.is_alive())
            print(queue.get())

        p.join()

    # def register_





if __name__ == "__main__":


    Main()

