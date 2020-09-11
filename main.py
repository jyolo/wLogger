from configparser import ConfigParser
from multiprocessing import Process,Queue
from server.start import start_server
import os,subprocess,time,json



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

        return conf


    def run(self):

        p = Process(
            target=start_server,
            name='web_server',
            args=(self.getFlaskConfig(),)
        )

        p.start()
        while True:
            time.sleep(1)
            # print(p.get())
            if(p.is_alive() == False):
                print( "11111farther pid: %s sonpid: %s is %s" % (os.getpid(),p.pid ,p.is_alive()))
                """
                    todo reboot de son proccess 
                """
                break
            else:
                print( "2222farther pid: %s sonpid: %s is %s" % (os.getpid(),p.pid ,p.is_alive()))


        p.join()


    def getFlaskConfig(self):
        flask_conf = {}
        for i in self.__config['flask'].items():
            if i[0].upper() == 'DEBUG':
                flask_conf[i[0].upper()] = self.__config['flask'].getboolean(i[0])
            else:
                flask_conf[i[0].upper()] = i[1]

        return flask_conf





if __name__ == "__main__":

    Main().run()

