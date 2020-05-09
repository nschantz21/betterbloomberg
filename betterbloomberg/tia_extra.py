from tia.bbg import LocalTerminal
import pandas as pd
import blpapi
import numpy as np

def get_ids(identifier, prepend='', fields=None, add_fields=None):
    """
    Helper Function to get a table of identifiers for a security.
    Recommended to call this once you have the set of Securities you're going to be working with.
    You can then keep it in memory and add to it as you do your work - makes it easy to switch between identifiers
    inputs
        identifier : array-like
            array of security identifiers
        prepend : str
            Prefix to add to identifiers, if not already present.
        fields : list
            List of ID fields to query
        add_fields : list
            Additional fields to add to query.
    returns : pandas DataFrame
        
    
    Example :
        import pandas as pd
        get_ids(pd.Series(data=['AAPL US EQUITY', '/bbgid/BBG000BBQCY0', '/isin/KYG960071028', '/cusip/594918104']))
    """
    if fields is None:
        fields = ['EQY_FUND_TICKER', 'ID_CUSIP', 
                  'ID_ISIN', 'ID_BB_GLOBAL', 'CENTRAL_INDEX_KEY_NUMBER']
    if add_fields is not None:
        fields.extend(add_fields)
    ids = prepend + identifier
    return LocalTerminal.get_reference_data(ids, fields, ignore_field_error=True, ignore_security_error=True).as_frame()



