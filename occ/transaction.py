class Transaction:
  def __init__(self, ts):
    self.start_ts = ts
    self.validation_ts = None
    self.finish_ts = None
    self.read_resources = []
    self.written_resources = []

  def read(self, resource):
    self.read_resources.append(resource)

  def write(self, resource):
    self.written_resources.append(resource)

  def commit(self):
    pass

  def abort(self):
    pass
