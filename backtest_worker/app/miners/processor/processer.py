from abc import ABC, abstractmethod

class Processor(ABC):
      def __init__(self, name: str):
          self.name = name
  
      @abstractmethod
      def execute(self,timestamp,data):
          raise NotImplementedError("Should implement this!")
  
      def __str__(self):
          return self.name