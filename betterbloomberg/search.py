import blpapi
from .core import BlpDataRequest
import pandas as pd


class Instrument(BlpDataRequest):
    service_type = "//blp/instruments"

    def __init__(self, **kwargs):
        super(Instrument, self).__init__(**kwargs)


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

    def __init__(self, query, ticker="", partial_match=True, max_results=100, **kwargs):
        """
        Government Security Lookup

        Parameters
        ----------
        query : str
            search query
        ticker : str
            ticker
        partial_match : bool

        max_results : int
            max results to return
        """
        self.query = query
        self.partial_match = partial_match
        self.ticker = ticker
        self.max_results = max_results

        super(Government, self).__init__(**kwargs)

    def generate_request(self):
        self.request.set("query", self.query)
        self.request.set("partialMatch", self.partial_match)
        self.request.set("ticker", self.ticker)
        self.request.set("maxResults", self.max_results)

    def process_response(self):
        RESULTS_DATA = blpapi.Name("results")

        govtData = (
            blpapi.event.MessageIterator(self.response).next().getElement(RESULTS_DATA)
        )

        sec_dict = dict()
        for i in range(govtData.numValues()):
            tmp_sec = govtData.getValueAsElement(i)
            p_key = tmp_sec.getElementAsString("parseky")
            sec_dict[p_key] = dict()
            for j in ["name", "ticker"]:
                sec_dict[p_key][j] = tmp_sec.getElementAsString(j)
        return sec_dict  # might need to be orient = "index"

    @property
    def data(self):
        if self.use_pandas:
            frame = pd.DataFrame.from_dict(self.__data, orient="index")
            frame.index.rename("sec_id", inplace=True)
            return frame
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data


class Security(Instrument):
    request_type = "instrumentListRequest"

    yk_filters = {
        "YK_FILTER_NONE",
        "YK_FILTER_CMDT",
        "YK_FILTER_EQTY",
        "YK_FILTER_MUNI",
        "YK_FILTER_PRFD",
        "YK_FILTER_CLNT",
        "YK_FILTER_MMKT",
        "YK_FILTER_GOVT",
        "YK_FILTER_CORP",
        "YK_FILTER_INDX",
        "YK_FILTER_CURR",
        "YK_FILTER_MTGE",
    }

    def __init__(
            self,
            query,
            yellow_key_filter="YK_FILTER_NONE",
            language_override="LANG_OVERRIDE_ENGLISH",
            max_results=100,
            **kwargs):
        """
        Security Instrument Search

        Parameters
        ----------
        query : str
            search query
        yellow_key_filter : str
            valid security field
        language_override : str
            valid language code
        max_results : int
            max results to return
        """
        self.query = query
        self.yellow_key_filter = yellow_key_filter
        self.language_override = language_override
        self.max_results = max_results
        super(Security, self).__init__(**kwargs)

    @property
    def yellow_key_filter(self):
        return self.__yellow_key_filter

    @yellow_key_filter.setter
    def yellow_key_filter(self, value):
        if value not in self.yk_filters:
            print(self.yk_filters)
            print(value)
            raise AttributeError
        else:
            self.__yellow_key_filter = value

    @yellow_key_filter.deleter
    def yellow_key_filter(self):
        del self.__yellow_key_filter

    def generate_request(self):
        self.request.set("query", self.query)
        self.request.set("yellowKeyFilter", self.yellow_key_filter)
        self.request.set("languageOverride", self.language_override)
        self.request.set("maxResults", self.max_results)

    def process_response(self):
        RESULTS_DATA = blpapi.Name("results")
        securityData = (
            blpapi.event.MessageIterator(self.response).next().getElement(RESULTS_DATA)
        )
        sec_dict = dict()
        for i in range(securityData.numValues()):
            tmp_sec = securityData.getValueAsElement(i)
            security = tmp_sec.getElementAsString("security")
            sec_dict[security] = tmp_sec.getElementAsString("description")
        return sec_dict

    @property
    def data(self):
        if self.use_pandas:
            frame = pd.DataFrame.from_dict(
                self.__data, orient="index", columns=["desc"]
            )
            # parse the security identifier
            frame.index = frame.index.str.replace("[<>]", " ").str.strip().str.upper()
            return frame
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data


class Curve(Instrument):
    """
    The Curve Lookup Request can retrieve a Curve based on its country code,
    currency code, type, subtype, Curve-specific ID and the Bloomberg ID for that
    Curve
    """

    request_type = "curveListRequest"

    def __init__(
            self,
            query,
            bbgid="",
            country_code="",
            currency_code="",
            curve_id="",
            curve_type=None,
            subtype=None,
            max_results=100,
            **kwargs):
        """
        Curve List Search Request

        Parameters
        ----------
        query : str
            query string
        bbgid : str
            Bloomberg ID
        country_code : str
            Bloomberg Country Code
        currency_code : str
            Bloomberg Currency Code
        curve_id : str
            Curve Id
        curve_type : str
            Curve Type
        subtype : str
            Curve Subtype
        max_results : int
            max results to return
        """
        self.query = query
        self.bbgid = bbgid
        self.country_code = country_code
        self.currency_code = currency_code
        self.curve_id = curve_id
        self.curve_type = curve_type
        self.subtype = subtype
        self.max_results = max_results

        super(Curve, self).__init__(**kwargs)

    def generate_request(self):
        self.request.set("query", self.query)
        self.request.set("bbgid", self.bbgid)
        self.request.set("countryCode", self.country_code)
        self.request.set("curveid", self.curve_id)
        self.request.set("maxResults", self.max_results)
        if self.curve_type is not None:
            self.request.set("type", self.curve_type)
        if self.subtype is not None:
            self.request.set("subtype", self.subtype)

    def process_response(self):
        RESULTS_DATA = blpapi.Name("results")

        securityData = (
            blpapi.event.MessageIterator(self.response).next().getElement(RESULTS_DATA)
        )
        sec_dict = dict()
        for i in range(securityData.numValues()):
            tmp_sec = securityData.getValueAsElement(i)
            curve_id = tmp_sec.getElementAsString("curve")
            sec_dict[curve_id] = dict()
            for j in [
                "description",
                "country",
                "currency",
                "curveid",
                "publisher",
                "bbgid",
            ]:
                sec_dict[curve_id][j] = tmp_sec.getElementAsString(j)
            for k in ["type", "subtype"]:
                data_list = list()
                for l in range(tmp_sec.getElement(k).numValues()):
                    data_list.append(tmp_sec.getElement(k).getValueAsString(l))
                sec_dict[curve_id][k] = data_list
        return sec_dict

    @property
    def data(self):
        if self.use_pandas:
            return pd.DataFrame.from_dict(self.__data, orient="index")
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data
