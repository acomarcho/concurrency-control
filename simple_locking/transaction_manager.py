from transaction import Transaction
from resource import Resource 


class TransactionManager: 
    def __init__(self, data):
        self.res = []
        self.transactions = {}
        self.resources = {}
        # Get all transactions involved
        for transaction in data.transactions: 
            self.transactions[transaction] = Transaction()
        # Get all resources involved
        for resource in data.resources: 
            self.resources[resource] = Resource()
        # Get schedule 
        self.schedule = data.schedule 

    def isLastOperation(self, sched, idx):
        # Return true if the next operation of the current transaction is commit, false otherwise
        for i in range(idx+1, len(self.schedule)):
            if self.schedule[i]['transaction'] == sched['transaction']:
                return self.schedule[i]['operation'] == 'commit'
    def isEqualNextsched(self, idx, sched):
        if self.schedule[idx+1]['operation'] != 'commit':
            return self.schedule[idx+1]['transaction'] == sched['transaction'] and self.schedule[idx+1]['resource'] == sched['resource']
        else:
            pass

    def run(self): 
        for idx, sched in enumerate(self.schedule): 
            # Commit will be skipped, handled with isLastOperation method
            # Assumption: Input format is correct 
            if ((sched['operation'] != 'commit')):
                # Exclusive lock
                if (self.resources[sched['resource']].isFree()):
                    self.resources[sched['resource']].lock(sched['transaction'])
                    print(f"Exclusive lock on resource {sched['resource']} given to Transaction T{sched['transaction']} ")
                    self.res.append(f"XL{sched['transaction']}({sched['resource']})") 
                if(self.resources[sched['resource']].isXLocked(sched['transaction'])):
                    if sched['operation'] == 'write':
                        self.res.append(f"W{sched['transaction']}({sched['resource']})")
                        print(f"Transaction T{sched['transaction']} writes {sched['resource']}")

                    elif sched['operation'] == 'read':
                        self.res.append(f"R{sched['transaction']}({sched['resource']})")
                        print(f"Transaction T{sched['transaction']} reads {sched['resource']}")
                    # Unlock
                    if (not self.isEqualNextsched(idx, sched)):
                        self.resources[sched['resource']].unlock()
                        self.res.append(f"UL{sched['transaction']}({sched['resource']})")
                        print(f"Exclusive lock on resource {sched['resource']} is unlocked")
                    # Directly commit if last operation
                    if self.isLastOperation(sched, idx):
                        self.res.append(f"C{sched['transaction']}")
                        self.transactions[sched['transaction']].commit()
                        print(f"Transaction T{sched['transaction']} commits")
        # Print Schedule
        print("Schedule with Simple Locking (exclusive locks only):")
        print('; '.join(self.res).strip() + ';')

