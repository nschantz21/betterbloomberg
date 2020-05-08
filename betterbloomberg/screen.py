import blpapi
import pandas as pd
from .reference_data import StaticReferenceData

class EQS(StaticReferenceData):
    request_type = "BeqsRequest"

    def __init__(self, name: str, screen_type: str, group: str, date=None, lang="ENGLISH", **kwargs):
        self.name = name
        self.screen_type = screen_type
        self.group = group
        self.date = date
        self.lang = lang
        super(EQS, self).__init__(**kwargs)

    def generate_request(self):
        self.request.set("screenName", self.name)
        self.request.set("screenType", self.screen_type)
        self.request.set("Group", self.group)
        self.request.set("languageId", self.lang)
        if self.date is not None:
            overrides = self.request.getElement("overrides")
            overrider = overrides.appendElement()
            overrider.setElement("fieldId", "PiTDate")
            overrider.setElement("value", self.date)


    def process_response(self):
        response_dict = dict()

        securities = (
            blpapi
            .event
            .MessageIterator(self.response)
            .next()
            .getElement("data")
            .getElement("securityData")
        )
        for i in range(securities.numValues()):
            temp_sec = securities.getValueAsElement(i)
            sec_id = temp_sec.getElement('security').getValue()
            response_dict[sec_id] = dict()
            sec_flds = temp_sec.getElement('fieldData')
            for field in sec_flds.elements():
                response_dict[sec_id][str(field.name())] = field.getValue()
        return response_dict

    @property
    def data(self):
        if self.use_pandas:
            frame = pd.DataFrame(self.__data).T
            return frame
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data

