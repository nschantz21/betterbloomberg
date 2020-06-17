import blpapi
import pandas as pd
from abc import ABCMeta, abstractmethod


class BlpDataRequest(object, metaclass=ABCMeta):
    use_pandas = True

    def __init__(
            self,
            host="localhost",
            port=8194,
            service_type=None,
            request_type=None,
            **kwargs):
        """
        Abstract Bloomberg Request Class
        """
        self.host = host
        self.port = port
        if service_type is None:
            self.service_type = self.__class__.service_type
        if request_type is None:
            self.request_type = self.__class__.request_type
        self.session = BlpDataRequest.session_handle(self.host, self.port)
        self.service = BlpDataRequest.service_handle(self.session, self.service_type)
        self.request = self.service.createRequest(self.request_type)
        self.generate_request()
        self.send_request()
        self.data = self.process_response()

    @property
    def data(self):
        """Handler for Data member. This is the processed response."""
        if self.use_pandas:
            return pd.DataFrame(self.__data)
        return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data

    @property
    @abstractmethod
    def service_type(self):
        pass

    @property
    @abstractmethod
    def request_type(self):
        pass

    @abstractmethod
    def generate_request(self):
        pass

    def send_request(self, correlation_id: int = None) -> None:
        """
        Send the constructed request through the service member.
        """
        eQ = blpapi.event.EventQueue()
        if correlation_id is not None:
            cid = blpapi.CorrelationId(correlation_id)
        self.session.sendRequest(self.request, eventQueue=eQ)
        while True:
            eventObj = eQ.nextEvent(timeout=500)
            if eventObj.eventType() == blpapi.event.Event.RESPONSE:
                # A RESPONSE Message indicates the request has been fully served
                break
        self.response = eventObj

    @abstractmethod
    def process_response(self):
        pass

    @staticmethod
    def session_handle(host: str, port: int):
        """Session handler. Add options"""
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        _session = blpapi.Session(sessionOptions)
        return _session

    @staticmethod
    def service_handle(session, service_type):
        """Service handler. Fail if session does not start."""
        sess = session
        if not sess.start():
            print("Failed to start session")
        # opens the service for the session
        if not sess.openService(service_type):
            print("failed to open {0} service".format(service_type))
        return sess.getService(service_type)
