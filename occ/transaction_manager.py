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
        # Handle writes -- Tinggal dibaca saja!
        resource = self.resources[task['resource']]
        transaction.write(resource)
        res.append(f"W{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} writes {task['resource']}")
        idx += 1
      elif task['operation'] == 'read':
        # Handle reads -- Tinggal ditulis saja!
        resource = self.resources[task['resource']]
        transaction.read(resource)
        res.append(f"R{task['transaction']}({task['resource']})")
        print(f"Transaction T{task['transaction']} reads {task['resource']}")
        idx += 1
      elif task['operation'] == 'commit':
        # Saat commit, update validation_ts dan finish_ts.
        # Anggapannya di sini adalah finish_ts = validation_ts untuk menyederhanakan.
        transaction.validation_ts = self.ts
        transaction.finish_ts = self.ts

        can_commit = True
        for key in self.transactions:
          # Cek setiap transaksi yang ada.
          to_check = self.transactions[key]
          if to_check != transaction:
            if to_check.validation_ts and to_check.validation_ts < transaction.validation_ts:
              # Jika ada sebuah transaksi yang waktu validasinya lebih dahulu,
              if to_check.finish_ts and to_check.finish_ts < transaction.start_ts:
                # Memenuhi syarat pertama -- OK
                continue
              elif to_check.finish_ts and transaction.start_ts < to_check.finish_ts and to_check.finish_ts < transaction.validation_ts:
                # Memenuhi syarat kedua -- Perlu dicek apakah transaksi to_check pernah menulis resource yang dibaca oleh transaction
                for r1 in to_check.written_resources:
                  if not can_commit:
                      break
                  for r2 in transaction.read_resources:
                    if not can_commit:
                      break
                    if r1 == r2:
                      # Ada resource yang pernah ditulis oleh to_check yang dibaca oleh transaction --> Rollback!
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
          # Jika diperbolehkan untuk commit,
          res.append(f"C{task['transaction']}")
          print(f"Transaction T{task['transaction']} commits")
          idx += 1
        else:
          # Tidak diperbolehkan untuk commit! Lakukan rollback.
          res.append(f"A{task['transaction']}")
          print(f"Transaction T{task['transaction']} aborts")
          aborted_operations = []
          for prev in range(idx):
            if self.transactions[self.schedule[prev]['transaction']] == transaction:
              aborted_operations.append(self.schedule[prev])

          # Mulai transaksi di timestamp baru
          self.transactions[task['transaction']] = Transaction(self.ts)

          # Abort transaksi -- rollback dari awal sampai titik kini.
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