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
from datetime import date

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


    def process_response(self):
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



class HistoricalDataRequest(ReferenceDataRequest):
    request_type = "HistoricalDataRequest"

    def __init__(self,
                 securities,
                 fields,
                 start,
                 end: str=None,
                 period: str="DAILY",
                 period_adjust: str="ACTUAL",
                 overrides=None,
                 **kwargs):
        """HistoricalDataRequest

        securities:
        fields:
        overrides:
        periodicity:
        start:
        end:
        overrides:

        """
        self.start = start
        if end is None:
            end = date.today().strftime("%Y%m%d")
        self.end = end
        self.period = period
        self.period_adjust = period_adjust
        super(HistoricalDataRequest, self).__init__(
            securities, fields, overrides, **kwargs
        )

    def generate_request(self):
        # call the parent generate request
        super(HistoricalDataRequest, self).generate_request()
        self.request.set("periodicitySelection", self.period)
        self.request.set("periodicityAdjustment", self.period_adjust)
        self.request.set("startDate", self.start)
        self.request.set("endDate", self.end)
        self.request.set("maxDataPoints", 100)

    def _process_response(self):
        print(self.request)
        while(True):
            event = self.session.nextEvent()
            msgIter = blpapi.event.MessageIterator(self.response)
            while(msgIter.next()):
                msg = msgIter.message()
                if (
                        (event.eventType() != blpapi.Event.PARTIAL_RESPONSE) &
                        (event.eventType() != blpapi.Event.RESPONSE)
                ):
                    continue
                securityData = msg.getElement("securityData")
                securityName = SecurityData.getElement("SecurityName")

                print(securityName)


    def process_response(self):
        data_dict = dict()
        for message in self.response:
            security_data = message.getElement("securityData")
            sec_id = security_data.getElement("security").getValue()
            record_list = list()
            field_data = security_data.getElement("fieldData")
            for i in range(field_data.numValues()):
                pt = field_data.getValue(i)
                record = dict()
                for element in pt.elements():
                    record[str(element.name())] = element.getValue()
                record_list.append(record)
            data_dict[sec_id] = record_list
        res_list = []
        for security, records in data_dict.items():
            res = pd.DataFrame(records).set_index("date")
            res.columns = pd.MultiIndex.from_tuples(
                [(security, x) for x in res.columns]
            )
            res_list.append(res)
        return pd.concat(res_list, axis=1)






