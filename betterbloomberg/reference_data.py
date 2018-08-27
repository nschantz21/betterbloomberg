
"""
Limitations exist on the number of fields for reference and historical data request: 400 fields for reference data request and 25 fields for historical data request. There is also a limit on the number of securities enforced by the Session’s MaxPendingRequests. API will split the securities in the request into groups of 10 securities and fields into groups of 128 fields. Therefore, depending of the number of securities and fields provided, the number of requests many exceed the default 1,024 MaxPendingRequests limit.
"""


import blpapi
from abc import ABCMeta, abstractmethod

    
# I think requires a class hierarchy
class BlpDataRequest(object):
    __metaclass__ = ABCMeta
    def __init__(self, host='localhost', port=8194, service_type=None, request_type=None):
        self.host = host
        self.port = port
        if service_type is not None:
            self.service_type = service_type
        if request_type is not None:
            self.request_type = request_type
        # not sure if I should make these static or class or instance methods
        self.session = BlpDataRequest.session_handle(self.host, self.port)
        self.service = BlpDataRequest.service_handle(self.session, self.service_type)
        self.request = self.service.createRequest(request_type)
    
    def send_request(self, correlation_id = None):
        eQ = blpapi.event.EventQueue()
        if correlation_id is not None:
            cid = blpapi.CorrelationId(correlation_id)
        self.session.sendRequest(self.request, eventQueue = eQ)
        while(True):
            eventObj = eQ.nextEvent()
            if eventObj.eventType() == blpapi.event.Event.RESPONSE:
                # A RESPONSE Message indicates the request has been fully served
                break
        return eventObj
    
    @abstractmethod
    def generate_request(self):
        pass
    
    @abstractmethod
    def process_request(self):
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
            print "Failed to start session"
        # opens the service for the session
        if (not sess.openService(service_type)):
            print "failed to open {0} service".format(service_type)
        return sess.getService(service_type)


class StaticReferenceData(BlpDataRequest):
    # this class is meant only to handle the service type argument of the 
    def __init__(self, *args, **kwargs):
        self.service_type = '//blp/refdata'
        super(StaticReferenceData, self).__init__(*args, **kwargs)
        

class ReferenceDataRequest(StaticReferenceData):
    def __init__(self, securities, fields, overrides=None, *args, **kwargs):
        self.request_type = "ReferenceDataRequest"
        self.securities = securities
        self.fields = fields
        if overrides is not None:
            self.overrides = overrides
        # I want all those good bits from the parent
        super(ReferenceDataRequest, self).__init__(*args, **kwargs)
    
    def generate_request(self):
        # constructing the request
        print "gen"
    
    def process_request(self):
        print "proc"
        

    