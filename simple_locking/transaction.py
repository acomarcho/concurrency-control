from resource import Resource
class Transaction: 
    def __init__(self): 
        self.commited = False
    
    def commit(self):
        self.commited = True
        pass