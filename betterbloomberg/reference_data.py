
"""
Limitations exist on the number of fields for reference and historical data request:  400 fields for reference data request and 25 fields for historical data request. There is also a limit on the number of securities enforced by the Session's MaxPendingRequests. API will split the securities in the request into groups of 10 securities and fields into groups of 128 fields. Therefore, depending of the number of securities and fields provided, the number of requests many exceed the default 1,024 MaxPendingRequests limit.
"""
import blpapi
from core import BlpDataRequest

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
        else:
            self.overrides = dict()
        super(ReferenceDataRequest, self).__init__(**kwargs)
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
        # return a dictionary of dictionaries
        pass
    
    
    # there are a lot of helper functions that could be made for this
    def process_response(self):
        my_dict = dict()
        y = blpapi.event.MessageIterator(self.response).next().getElement("securityData")
        for i in xrange(y.numValues()):
            temp_sec = y.getValueAsElement(i)
            sec_id = temp_sec.getElement('security').getValue()
            my_dict[sec_id] = dict()
            sec_flds = temp_sec.getElement('fieldData')
            for j in sec_flds.elements():
                my_dict[sec_id][str(j.name())] = j.getValue()
        return my_dict
    
    