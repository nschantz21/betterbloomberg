from .field import *
from .portfolio import *
from .reference_data import *
from .screen import *
from .search import *
from .study import *

def (kind: str="ReferenceDataRequest", **kwargs):
    """
    Wrapper function for BetterBloomberg data requests.


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

    return request_dict[kind](**kwargs).data
