class Resource: 
    def __init__(self):
        self.transaction = ""
        # Status true berarti tidak di lock oleh transaksi apapun
        self.status = True 
        pass
    def lock(self, transaction):
        self.status = False
        self.transaction = transaction
    
    def unlock(self):
        self.status = True
        self.transaction = ""
    
    def isNotXLocked(self):
        return self.status
