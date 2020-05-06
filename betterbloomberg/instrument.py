import blpapi
from .core import BlpDataRequest
import pandas as pd
from datetime import date

class Instrument(BlpDataRequest):
    service_type = "//blp/instruments"

    def __init__(self, **kwargs):
        super(Instrument, self).__init__(**kwargs)

    @property
    def query(self):
        return self.__query

    @query.setter
    def query(self, value):
        self.__query = value

    @query.deleter
    def query(self):
        del __query


class Government(Instrument):
    """
    The Government Lookup Request searches through government securities.
    As with all Requests, users can specify the "query" string and the maximum
    number of results. As every government security has a Ticker that is not
    unique, these securities can also be filtered by this Ticker. For example, a
    user can specify filter Tickers equal to "T" or set partial match
    (i.e., "partialMatch") to true and filter out all government securities
    beginning with the letter "T" by setting the "query" element value to "T*"
    - kinda the opposite of regex... my god, why did they do this...
    """

    request_type = "govtListRequest"

    def __init__(
            self,
            query,
            ticker="",
            partial_match=True,
            max_results=100,
            **kwargs):
        self.query = query
        self.partial_match = partial_match
        self.ticker = ticker
        self.max_results = max_results

        super(Government, self).__init__(**kwargs)

    def generate_request(self):
        self.request.set("query", self.query)
        self.request.set('partialMatch', self.partial_match)
        self.request.set('ticker', self.ticker)
        self.request.set('maxResults', self.max_results)

    def process_response(self):
        RESULTS_DATA = blpapi.Name("results")

        govtData = (
            blpapi
            .event
            .MessageIterator(self.response)
            .next()
            .getElement(RESULTS_DATA)
        )

        sec_dict = dict()
        for i in range(govtData.numValues()):
            tmp_sec = govtData.getValueAsElement(i)
            p_key = tmp_sec.getElementAsString('parseky')
            sec_dict[p_key] = dict()
            for j in ['name', 'ticker']:
                sec_dict[p_key][j] = tmp_sec.getElementAsString(j)
        return sec_dict



class Security(Instrument):
    request_type = "instrumentListRequest"


class Curve(Instrument):
    request_type = "curveListRequest"

