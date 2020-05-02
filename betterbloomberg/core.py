import blpapi
import pandas as pd
from abc import ABCMeta, abstractmethod


class BlpDataRequest(object, metaclass=ABCMeta):
    # Abstract Base Class for Creating requests
    use_pandas = True

    def __init__(
            self,
            host='localhost',
            port=8194,
            service_type=None,
            request_type=None,
            **kwargs
    ):
        self.host = host
        self.port = port
        if service_type is None:
            self.service_type = self.__class__.service_type
        if request_type is None:
            self.request_type = self.__class__.request_type
        # not sure if I should make these static or class or instance methods
        self.session = BlpDataRequest.session_handle(self.host, self.port)
        self.service = BlpDataRequest.service_handle(self.session, self.service_type)
        self.request = self.service.createRequest(self.request_type)
        # not sure if these should be included at this level
        self.generate_request()
        self.send_request()
        self.data = self.process_response()

    @abstractmethod
    def service_type(self):
        pass

    @abstractmethod
    def request_type(self):
        pass

    @abstractmethod
    def generate_request(self):
        pass

    # I don't think this needs to be an abstract method
    def send_request(self, correlation_id=None):
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

    def get_data(self):
        if self.use_pandas:
            self.data = pd.DataFrame(self.data)
        return self.data

    @abstractmethod
    def process_response(self):
        pass

    @staticmethod
    def session_handle(host, port):
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        _session = blpapi.Session(sessionOptions)
        return _session

    @staticmethod
    def service_handle(session, service_type):
        sess = session
        if (not sess.start()):
            print("Failed to start session")
        # opens the service for the session
        if (not sess.openService(service_type)):
            print("failed to open {0} service".format(service_type))
        return sess.getService(service_type)
