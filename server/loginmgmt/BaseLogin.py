import logging
class BaseLogin:

  def __init__(self):
    # self.brokerAppDetails = brokerAppDetails
    self.brokerHandle = {}

  # Derived class should implement login function and return redirect url
  def login(self, args):
    pass

  def setBrokerHandle(self,id,brokerHandle):
    self.brokerHandle[id] = brokerHandle
    logging.info(f"Login Complete for {id}")

  def setAccessToken(self, accessToken):
    self.accessToken = accessToken

  # def getBrokerAppDetails(self):
  #   return self.brokerAppDetails

  # def getAccessToken(self):
  #   return self.accessToken

  def getBrokerHandle(self,id):
    return self.brokerHandle#[id]