class InputParser:
  def __init__(self):
    self.transactions = []
    self.resources = []
    self.schedule = []
    pass

  def parse(self, input):
    # Split input berdasarkan '; '
    tasks = input.split(';')
    # Proses setiap task
    for task in tasks:
      # Lakukan stripping lebih dahulu
      task = task.lstrip()
      task = task.rstrip(';')
      if (len(task) == 0):
        break
      # Ambil data transaksi dan resource
      transaction = task[1]
      resource = task.split('(')[0].rstrip(')')
      # Proses transaksi
      if transaction not in self.transactions:
        self.transactions.append(transaction)
      # Proses resource
      if resource not in self.resources:
        self.resources.append(resource)
      # Proses schedule
      if (task[0] == 'W'):
        self.schedule.append({"transaction": transaction, "resource": resource, "operation": "write"})
      elif (task[0] == 'R'):
        self.schedule.append({"transaction": transaction, "resource": resource, "operation": "read"})
      elif (task[0] == 'C'):
        self.schedule.append({"transaction": transaction, "operation": "commit"})
      else:
        raise Exception(f"Invalid task {task}")