from transaction import Transaction
from resource import Resource
import time

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
        idx += 1
      elif task['operation'] == 'read':
        resource = self.resources[task['resource']]
        transaction.read(resource)
        res.append(f"R{task['transaction']}({task['resource']})")
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
                  for r2 in transaction.read_resources:
                    if r1 == r2:
                      can_commit = False
                if not can_commit:
                  break
              else:
                can_commit = False
                break
          
        if can_commit:
          res.append(f"C{task['transaction']}")
          idx += 1
        else:
          res.append(f"A{task['transaction']}")
          newSchedule = []
          for prev in range(idx):
            if not self.transactions[self.schedule[prev]['transaction']].finish_ts:
              newSchedule.append(self.schedule[prev])
            elif self.transactions[self.schedule[prev]['transaction']] == transaction:
              newSchedule.append(self.schedule[prev])
          for next in range(idx, len(self.schedule)):
            newSchedule.append(self.schedule[next])

          for newIndex, entry in enumerate(newSchedule):
            if self.transactions[entry['transaction']] == transaction:
              idx = newIndex
              break

          self.transactions[task['transaction']] = Transaction(self.ts)
          self.schedule = newSchedule
          
    print('; '.join(res).strip())