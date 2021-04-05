import blpapi
import pandas as pd
from .reference_data import StaticReferenceData

__all__ = ["PortfolioDataRequest", ]

class PortfolioDataRequest(StaticReferenceData):
    request_type = "PortfolioDataRequest"

    def __init__(self, port_id: str, field: str="PORTFOLIO_MWEIGHT", ref_date: str=None, **kwargs):
        """
        Portfolio Data Request

        Paramters
        ---------
        port_id : str
            Portfolio ID found through the PRTU function in the Terminal.
            Format is "Uxxxxxxxx-xx Client".
        field : str
            The field to associate with each position
        ref_date : str
            Reference date for the portfolio holdings.
            Format is "YYYYMMDD"
        """
        self.port_id = port_id
        self.field = field
        self.ref_date = ref_date
        super(PortfolioDataRequest, self).__init__(**kwargs)

    def generate_request(self):
        securities = self.request.getElement("securities")
        securities.appendValue(self.port_id)

        pFields = self.request.getElement("fields")
        pFields.appendValue(self.field)

        if self.ref_date is not None:
            overrides = self.request.getElement("overrides")
            overrider = overrides.appendElement()
            overrider.setElement("fieldId", "REFERENCE_DATE")
            overrider.setElement("value", self.ref_date)

    def process_response(self):
        RESULTS_DATA = blpapi.Name("securityData")

        securityData = (
            blpapi.event.MessageIterator(self.response).next().getElement(RESULTS_DATA)
        )
        secdata2 = securityData.getValue(0)
        fld_data = secdata2.getElement("fieldData")
        positions = fld_data.getElement(self.field)
        port_dict = {}

        for i in range(positions.numValues()):
            pos = positions.getValue(i)
            sec_name = pos.getElementAsString("Security")
            sec_weight = pos.getElementAsString("Weight")
            port_dict[sec_name] = sec_weight

        return port_dict

    @property
    def data(self):
        if self.use_pandas:
            frame = pd.DataFrame.from_dict(self.__data, orient="index")
            frame = frame[0].rename("weight").astype(float)
            return frame
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data
