
"""
Limitations exist on the number of fields for reference and historical data request:  400 fields for reference data request and 25 fields for historical data request. There is also a limit on the number of securities enforced by the Session's MaxPendingRequests. API will split the securities in the request into groups of 10 securities and fields into groups of 128 fields. Therefore, depending of the number of securities and fields provided, the number of requests many exceed the default 1,024 MaxPendingRequests limit.
"""
import blpapi # I think you can import this at the module level
import pandas as pd
from abc import ABCMeta, abstractmethod, abstractproperty


class BlpDataRequest(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, host='localhost', port=8194, service_type=None, request_type=None, **kwargs):
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
        self.session.stop()
    
    # add extra logic to the the destructor to close the service
       
    @abstractproperty
    def service_type(self):
        pass
    
    @abstractproperty
    def request_type(self):
        pass
    
    @abstractmethod
    def generate_request(self):
        pass
    
    # I don't think this needs to be an abstract method
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
        self.response = eventObj

    
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
            print "Failed to start session"
        # opens the service for the session
        if (not sess.openService(service_type)):
            print "failed to open {0} service".format(service_type)
        return sess.getService(service_type)
        


class StaticReferenceData(BlpDataRequest):
    # this class is meant only to handle the service type argument
    service_type = '//blp/refdata'
    
    def __init__(self, **kwargs):
        super(StaticReferenceData, self).__init__(**kwargs)
        

class ReferenceDataRequest(StaticReferenceData):
    request_type = "ReferenceDataRequest"
    
    def __init__(self, securities, fields, overrides=None, **kwargs):
        """
        securities : array-like
            security identifiers
        fields : array-like
            reference fields
        overrides : dict
            override fields and values
        """
        self.securities = securities
        self.fields = fields
        if overrides is not None:
            self.overrides = overrides
        super(ReferenceDataRequest, self).__init__(**kwargs)
        # self.session
        # self.service
        # self.request
        # self.response
    
    def generate_request(self):
        # constructing the request
        for s in self.securities:
            self.request.append("securities", s)
        for f in self.fields:
            self.request.append("fields", f)
        # setting the overrides is a bitch.
        # might be a better way to do this
        ovrds = self.request.getElement("overrides")
        for (k, v) in self.overrides.iteritems():
            ovr = ovrds.appendElement()
            ovr.setElement("fieldId", k)
            ovr.setElement("value", v)
    
    def processBulkField(refBulkfield):
        # get the total number of Bulk data points
        # get the number of subfields for each bulk data element
        # read each field in bulk data
        
        pass
    
    
    
    def process_response(self):
        SECURITY_DATA = blpapi.Name("fieldDate")
        FIELD_DATA = blpapi.Name("fieldInfo")
        securityData = blp.event.MessageIterator(self.response).next.getElement(SECURITY_DATA)
        my_dict = dict()
        for i in xrange(securityData.numValues()):
            tmp_sec = securityData.getValueAsElement(i)
            fid = tmp_sec.getElementAsString("id")
            my_dict[fid] = dict()
            for f in ['mnemonic', 'description', 'documentation']:
                my_dict[fid][f] = tmp_sec.getElement(FIELD_DATA).getElementAsString(f)
            if overrides:
                ovrd_list = list()
                for j in xrange(tmp_sec.getElement(FIELD_DATA).getElement('overrides').numValues()):
                    ovrd_list.append(tmp_sec.getElement(FIELD_DATA).getElement('overrides').getValue(j))
                my_dict[fid]['overrides'] = ovrd_list 
        return pd.DataFrame.from_dict(my_dict, orient='index')

    