import logging

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "#配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S ' #配置输出时间的格式，注意月份和天数不要搞乱了

logging.basicConfig(level=logging.DEBUG,format=LOG_FORMAT, datefmt = DATE_FORMAT,filename=r"./error.log" )


class Base():
    logging = logging

