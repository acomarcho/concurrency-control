from transaction import Transaction
from resource import Resource
from version import Version

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
      if task['operation'] == 'read':
        resource = self.resources[task['resource']]
        latest_version = None
        for version in resource.versions:
          if version.w_ts <= transaction.ts:
            if not latest_version:
              latest_version = version
            else:
              if version.w_ts > latest_version.w_ts:
                latest_version = version
        if latest_version.r_ts < transaction.ts:
          latest_version.r_ts = transaction.ts
        transaction.read(resource)
        res.append(f"R{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} reads {task['resource']}")
        idx += 1
      elif task['operation'] == 'write':
        resource = self.resources[task['resource']]
        latest_version = None
        for version in resource.versions:
          if version.w_ts <= transaction.ts:
            if not latest_version:
              latest_version = version
            else:
              if version.w_ts > latest_version.w_ts:
                latest_version = version
        if transaction.ts < latest_version.r_ts:
          res.append(f"A{op['transaction']}")
          print(f"Transaction T{op['transaction']} aborts")
          aborted_operations = []
          for prev in range(idx):
            if self.transactions[self.schedule[prev]['transaction']] == transaction:
              aborted_operations.append(self.schedule[prev])
          self.transactions[task['transaction']] = Transaction(self.ts)
          for op in aborted_operations:
            transaction = self.transactions[op['transaction']]
            resource = self.resources[op['resource']]
            latest_version = None
            for version in resource.versions:
              if version.w_ts <= transaction.ts:
                if not latest_version:
                  latest_version = version
                else:
                  if version.w_ts > latest_version.w_ts:
                    latest_version = version
            if latest_version.r_ts < transaction.ts:
              latest_version.r_ts = transaction.ts
            if op['operation'] == 'read':
              transaction.read(resource)
              res.append(f"R{op['transaction']}({op['resource']})")
              print(f"Transaction T{op['transaction']} reads {op['resource']}")
            elif op['operation'] == 'write':
              if transaction.ts == latest_version.w_ts:
                pass
              else:
                new_version = Version(transaction.ts, transaction.ts)
                resource.versions.append(new_version)
              res.append(f"W{task['transaction']}({task['resource']})")
              print(f"Transaction T{task['transaction']} writes {task['resource']}")
          continue
        elif transaction.ts == latest_version.w_ts:
          pass
        else:
          new_version = Version(transaction.ts, transaction.ts)
          resource.versions.append(new_version)
        transaction.write(resource)
        res.append(f"W{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} writes {task['resource']}")
        idx += 1
      elif task['operation'] == 'commit':
        res.append(f"C{task['transaction']}")
        print(f"Transaction T{task['transaction']} commits")
        idx += 1

    print("Schedule with MVCC:")
    print('; '.join(res).strip() + ';')