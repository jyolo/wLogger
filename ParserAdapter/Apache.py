from ParserAdapter.BaseAdapter import Adapter

#日志格式官方文档 http://httpd.apache.org/docs/2.4/logs.html
#日志切割官方文档 http://httpd.apache.org/docs/2.4/programs/rotatelogs.html

class Handler(Adapter):

    def __init__(self,*args ,**kwargs):
        super(Handler,self).__init__(*args,**kwargs)
        print('apache')






