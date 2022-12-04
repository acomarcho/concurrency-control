class Resource: 
    def __init__(self):
        self.transaction = ""
        # Status true berarti tidak di lock oleh transaksi apapun
        self.status = False 
        pass
    def lock(self, transaction):
        self.status = True
        self.transaction = transaction
    
    def unlock(self):
        self.status = False
        self.transaction = ""
    def isFree(self):
        return self.transaction == ""
    def isXLocked(self, transaction):
        return self.status and self.transaction == transaction
