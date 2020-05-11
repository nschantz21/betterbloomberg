import blpapi
from .core import BlpDataRequest
import pandas as pd
import datetime


class Study(BlpDataRequest):
    service_type = "//blp/tasvc"
    request_type = "studyRequest"

    def __init__(
            self,
            security,
            study,
            start,
            end,
            data_range="historical",
            interval=15,
            **kwargs):
        """
        Technical Analysis Study

        This portion of the API is sparsely documented, although it does work.
        You will have to guess some parameters, but it is really useful if you're
        interested in technical studies.

        Parameters
        ----------
        security : str
            Security ID
        study : str
            Name of the study
        start : str
            YYYYMMDD
        end : str
            YYYYMMDD
        data_range : str
            Valid date range. Just always use historical. Also allows
        interval : int
            seemingly doesn't matter for historical
        """
        self.security = security
        self.study = study
        self.start = start
        self.end = end
        self.data_range = data_range
        self.interval = interval
        self.kwargs = kwargs
        super(Study, self).__init__()

    def generate_request(self):
        # set security name
        (
            self.request.getElement("priceSource")
            .getElement("securityName")
            .setValue(self.security)
        )
        # set data range
        (
            self.request.getElement("priceSource")
            .getElement("dataRange")
            .setChoice(self.data_range)
        )
        historical_element = (
            self.request.getElement("priceSource")
            .getElement("dataRange")
            .getElement("historical")
        )

        # start
        historical_element.getElement("startDate").setValue(self.start)
        # end
        historical_element.getElement("endDate").setValue(self.end)

        # study attributes
        self.request.getElement("studyAttributes").setChoice(
            self.study + "StudyAttributes"
        )
        study_attributes = self.request.getElement("studyAttributes").getElement(
            self.study + "StudyAttributes"
        )

        # study interval
        # study_attributes.getElement("period").setValue(self.interval)

        # data price sources
        # idk where you find the naming conventions for this
        for k, v in self.kwargs.items():
            study_attributes.getElement(k).setValue(v)

    def process_response(self):
        record_list = []
        for msg in self.response:
            study_data = msg.getElement("studyData")
            for record in range(study_data.numValues()):
                record_data = study_data.getValue(record)
                record_dict = dict()
                for elem in record_data.elements():
                    record_dict[str(elem.name())] = elem.getValue()
                record_list.append(record_dict)
        return record_list

    @property
    def data(self):
        if self.use_pandas:
            frame = pd.DataFrame(self.__data)
            frame.set_index("date")
            return frame
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data
