from abc import ABC,abstractmethod

class login_Engine(ABC):
    
    def __init__(self) -> None:
        super().__init__
    
    @abstractmethod
    def setup():
        pass
    


    
class Checking_Engine(ABC):
    
    def __init__(self) -> None:
        super().__init__
    
    @abstractmethod
    def check():
        pass

