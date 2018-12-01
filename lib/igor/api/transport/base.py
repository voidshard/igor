import abc

from igor.api.gateway import IgorGateway


class Base(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def serve(self, gate: IgorGateway):
        pass

    @abc.abstractmethod
    def stop(self):
        pass
