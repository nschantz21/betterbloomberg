"""
Limitations exist on the number of fields for reference and historical data request:
    400 fields for reference data request and 25 fields for historical data request.
There is also a limit on the number of securities enforced by the Session's
MaxPendingRequests.

API will split the securities in the request into groups of
10 securities and fields into groups of 128 fields. Therefore, depending of the
number of securities and fields provided, the number of requests many exceed the
default 1,024 MaxPendingRequests limit.
"""
import blpapi
from .core import BlpDataRequest
import pandas as pd


class StaticReferenceData(BlpDataRequest):
    # this class is meant only to handle the service type argument
    service_type = '//blp/refdata'

    def __init__(self, **kwargs):
        super(StaticReferenceData, self).__init__(**kwargs)


class ReferenceDataRequest(StaticReferenceData):
    request_type = "ReferenceDataRequest"

    def __init__(self, securities, fields, overrides=None, **kwargs):
        """ReferenceDataRequest



        securities : array-like, str
            security identifiers
        fields : array-like, str
            reference fields
        overrides : dict
            override fields and values

        """
        if type(securities) == list:
            self.securities = securities
        else:
            self.securities = [securities, ]
        if type(fields) == list:
            self.fields = fields
        else:
            self.fields = [fields, ]
        if overrides is not None:
            self.overrides = overrides
        else:
            self.overrides = dict()
        super(ReferenceDataRequest, self).__init__(**kwargs)


    def generate_request(self):
        # constructing the request
        for s in self.securities:
            self.request.append("securities", s)
        for f in self.fields:
            self.request.append("fields", f)
        # setting the overrides is a bitch.
        # might be a better way to do this
        ovrds = self.request.getElement("overrides")
        for (k, v) in self.overrides.items():
            ovr = ovrds.appendElement()
            ovr.setElement("fieldId", k)
            ovr.setElement("value", v)


    @staticmethod
    def process_bulk_field(refBulkfield):
        response_list = []
        numofBulkValues = refBulkfield.numValues()
        for bvCtr in range(numofBulkValues):
            bulkElement = refBulkfield.getValueAsElement(bvCtr)
            # get the number of subfields for each bulk data element
            numofBulkElements = bulkElement.numElements()
            bulk_dict = dict()
            for beCtr in range(numofBulkElements):
                elem = bulkElement.getElement(beCtr)
                bulk_dict[str(elem.name())] = elem.getValue()
            response_list.append(bulk_dict)
        return response_list


    # there are a lot of helper functions that could be made for this
    def process_response(self, blk=False):
        # determine if bulk data has been received
        response_dict = dict()
        securities = (
            blpapi
                .event
                .MessageIterator(self.response)
                .next()
                .getElement("securityData")
        )
        # iterate through the securities
        for i in range(securities.numValues()):
            temp_sec = securities.getValueAsElement(i)
            sec_id = temp_sec.getElement('security').getValue()
            response_dict[sec_id] = dict()
            sec_flds = temp_sec.getElement('fieldData')
            # iterate through fields
            for field in sec_flds.elements():
                # bulk data processing
                if field.isArray():
                    bulk_data = pd.DataFrame(
                        ReferenceDataRequest.process_bulk_field(field)
                    )
                    response_dict[sec_id][str(field.name())] = bulk_data

                else:
                    response_dict[sec_id][str(field.name())] = field.getValue()
        return response_dict

