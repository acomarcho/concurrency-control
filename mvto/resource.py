from version import Version

class Resource:
  def __init__(self):
    start_version = Version(0, 0)
    self.versions = [start_version]