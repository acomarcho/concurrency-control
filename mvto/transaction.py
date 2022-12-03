class Transaction:
  def __init__(self, ts):
    self.ts = ts

  def read(self, resource):
    pass

  def write(self, resource):
    pass

  def commit(self):
    pass

  def abort(self):
    pass
