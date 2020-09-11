from ParserAdapter.BaseAdapter import Adapter

class Handler(Adapter):

    def __init__(self,*args ,**kwargs):
        super(Handler,self).__init__(*args,**kwargs)
        print('apache')






