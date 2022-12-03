from transaction import Transaction
from resource import Resource

class TransactionManager:
  def __init__(self, data):
    self.transactions = {}
    self.resources = {}

    self.ts = 1
    
    for transaction in data.transactions:
      self.transactions[transaction] = Transaction(self.ts)
      self.ts += 1
    
    for resource in data.resources:
      self.resources[resource] = Resource()

    self.schedule = data.schedule

  def run(self):
    res = []
    idx = 0
    while idx < len(self.schedule):      
      self.ts += 1
      task = self.schedule[idx]
      transaction = self.transactions[task['transaction']]
      if task['operation'] == 'write':
        resource = self.resources[task['resource']]
        transaction.write(resource)
        res.append(f"W{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} writes {task['resource']}")
        idx += 1
      elif task['operation'] == 'read':
        resource = self.resources[task['resource']]
        transaction.read(resource)
        res.append(f"R{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} reads {task['resource']}")
        idx += 1
      elif task['operation'] == 'commit':
        transaction.validation_ts = self.ts
        transaction.finish_ts = self.ts

        can_commit = True
        for key in self.transactions:
          to_check = self.transactions[key]
          if to_check != transaction:
            if to_check.validation_ts and to_check.validation_ts < transaction.validation_ts:
              if to_check.finish_ts and to_check.finish_ts < transaction.start_ts:
                continue
              elif to_check.finish_ts and transaction.start_ts < to_check.finish_ts and to_check.finish_ts < transaction.validation_ts:
                for r1 in to_check.written_resources:
                  if not can_commit:
                      break
                  for r2 in transaction.read_resources:
                    if not can_commit:
                      break
                    if r1 == r2:
                      conflicting_resource = ""
                      for resource in self.resources:
                        if self.resources[resource] == r1:
                          conflicting_resource = resource
                          break
                      print(f"Transaction T{task['transaction']} cannot commit, conflicting with T{key} on resource {conflicting_resource}")
                      can_commit = False
                if not can_commit:
                  break
              else:
                can_commit = False
                break
          
        if can_commit:
          res.append(f"C{task['transaction']}")
          print(f"Transaction T{task['transaction']} commits")
          idx += 1
        else:
          res.append(f"A{task['transaction']}")
          print(f"Transaction T{task['transaction']} aborts")
          aborted_operations = []
          for prev in range(idx):
            if self.transactions[self.schedule[prev]['transaction']] == transaction:
              aborted_operations.append(self.schedule[prev])

          self.transactions[task['transaction']] = Transaction(self.ts)

          for op in aborted_operations:
            transaction = self.transactions[op['transaction']]
            if op['operation'] == 'write':
              resource = self.resources[op['resource']]
              transaction.write(resource)
              res.append(f"W{op['transaction']}({op['resource']})")
              print(f"Transaction T{op['transaction']} writes {op['resource']}")
            elif op['operation'] == 'read':
              resource = self.resources[op['resource']]
              transaction.read(resource)
              res.append(f"R{op['transaction']}({op['resource']})")
              print(f"Transaction T{op['transaction']} reads {op['resource']}")
    
    print("Schedule with OCC:")
    print('; '.join(res).strip() + ';')