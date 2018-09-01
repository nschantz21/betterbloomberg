
"""
Limitations exist on the number of fields for reference and historical data request:  400 fields for reference data request and 25 fields for historical data request. There is also a limit on the number of securities enforced by the Session's MaxPendingRequests. API will split the securities in the request into groups of 10 securities and fields into groups of 128 fields. Therefore, depending of the number of securities and fields provided, the number of requests many exceed the default 1,024 MaxPendingRequests limit.
"""
import blpapi
import pandas as pd
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
        # reutrn a dictionary of dictionaries
        pass
    
    
    # there are a lot of helper functions that could be made for this
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

    