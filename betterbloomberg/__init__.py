from .field import *
from .portfolio import *
from .reference_data import *
from .screen import *
from .search import *
from .study import *


def get(req_type: str = "ReferenceDataRequest", **kwargs):
    """
    Wrapper function for BetterBloomberg data requests.

    This is a simple function to call one of the available Bloomberg Services
    and return the data member of the class. Look at the function's
    `request_dict` data member to see what is available.

    Parameters
    ----------
    req_type : str
        The request type e.g. for reference data use 'ReferenceDataRequest'
    **kwargs :
        relevant parameters and arguments for the request specified in the
        'request_type' argument.

    Returns
    -------
    pd.DataFrame
    """
    request_dict = {
        "ReferenceDataRequest": ReferenceDataRequest,
        "HistoricalDataRequest": HistoricalDataRequest,
        "FieldSearch": FieldSearch,
        "FieldInfo": FieldInfo,
        "PortfolioDataRequest": PortfolioDataRequest,
        "EQS": EQS,
        "GovernmentSearch": GovernmentSearch,
        "CurveSearch": CurveSearch,
        "SecuritySearch": SecuritySearch,
        "Study": Study
    }

    return request_dict[req_type](**kwargs).data
