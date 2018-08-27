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




class Window(object):
    def __init__(self, name):
        self.name = name

    def displaySecurityInfo(self, msg):
        print "%s: %s" % (self.name, msg)

        
        
def easyRequest(sess, req, **kwargs):
    """
    sess : Session
    req : Request
    **kwargs : dict
    """
    for (k, v) in kwargs.iteritems():
        for i in v:
            req.append(k, i)
    eventQueue = blpapi.event.EventQueue()
    secInfoWindow = Window("SecurityInfo")
    cid = blpapi.CorrelationId(secInfoWindow)
    sess.sendRequest(req, correlationId = cid, eventQueue = eventQueue)
    while(True):
        eventObj = eventQueue.nextEvent()
        if eventObj.eventType() == blpapi.event.Event.RESPONSE:
            # A RESPONSE Message indicates the request has been fully served
            break
    return eventObj

def field_information(field_id, docs=False, overrides=False, print_request=False, verbose=False):
    """
    Field Information Request: Provides a description of the specified fields in the request.
    
    Parameters:
        field_id : string
            Fields can be specified as an alpha numeric or mnemonic.
        docs : bool
            Returns a description about the field as seen on FLDS <GO>. Default value is false.
        overrides : bool
            Returns a value for the element that describes the behavior of the field requested.  It will give a list of overrides for that field
        
    Returns : dataframe
        it's pretty self-explanatory
    """
    
    # these are already the default so you don't really need to do this
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)

    # starts the session
    session = blpapi.Session(sessionOptions)
    if (not session.start()):
        print "Failed to start session"

    # opens the service for the session
    if (not session.openService("//blp/apiflds")):
        print "failed to open //blp/fields service"

    # get the service
    fieldDataService = session.getService("//blp/apiflds")
    request = fieldDataService.createRequest("FieldInfoRequest")
    
    for fid in field_id:
        request.append("id", fid)
    request.set("returnFieldDocumentation", docs)
    if overrides:
            request.append("properties", "fieldoverridable")
            
    if print_request:
        print request
    
    my_event = easyRequest(session, request)
    del request
    
    my_dict = dict()
    
    SECURITY_DATA = blpapi.Name("fieldData")
    FIELD_DATA = blpapi.Name("fieldInfo")

    securityData = blpapi.event.MessageIterator(my_event).next().getElement(SECURITY_DATA)

    for i in xrange(securityData.numValues()):
        tmp_sec = securityData.getValueAsElement(i)
        fid = tmp_sec.getElementAsString("id")
        my_dict[fid] = dict()
        for f in ['mnemonic', 'description', 'documentation']:
            my_dict[fid][f] = tmp_sec.getElement(FIELD_DATA).getElementAsString(f)
        if overrides:
            ovrd_list = list()
            for j in xrange(tmp_sec.getElement(FIELD_DATA).getElement('overrides').numValues()):
                ovrd_list.append(tmp_sec.getElement(FIELD_DATA).getElement('overrides').getValue(j))
            my_dict[fid]['overrides'] = ovrd_list 
    
    if verbose:
        for msg in my_event:
            print msg
    
    field_frame = pd.DataFrame.from_dict(my_dict, orient='index')
    
    session.stop()
    
    return field_frame


def government_lookup(query, ticker='', partialMatch=True, maxResults=100, print_request=False):
    """
    The Government Lookup Request searches through government securities.
    As with all Requests, users can specify the "query" string and the maximum number of results.
    As every government security has a Ticker that is not unique, these securities can also be filtered by this Ticker. 
    For example, a user can specify filter Tickers equal to "T" or set partial match (i.e., "partialMatch") to true and filter out all government securities beginning with the letter "T" by setting the "query" element value to "T*" - kinda the opposite of regex... my god, why did they do this...
    
    """
    
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)
    
    # starts the session
    session = blpapi.Session(sessionOptions)
    if (not session.start()):
        print "Failed to start session"
    
    # opens the service for the session
    if (not session.openService("//blp/instruments")):
        print "failed to open //blp/instruments service"
    
    # get the service
    secfService = session.getService("//blp/instruments")
    request = secfService.createRequest("govtListRequest")
    
    request.set('query', query)
    request.set('partialMatch', partialMatch)
    request.set('ticker', ticker)
    request.set('maxResults', maxResults)
    
    test_event = easyRequest(session, request)
    
    if print_request:
        print request
    del request

    RESULTS_DATA = blpapi.Name("results")

    govtData = blpapi.event.MessageIterator(test_event).next().getElement(RESULTS_DATA)

    sec_dict = dict()
    for i in xrange(govtData.numValues()):
        tmp_sec = govtData.getValueAsElement(i)
        p_key = tmp_sec.getElementAsString('parseky')
        sec_dict[p_key] = dict()
        for j in ['name', 'ticker']:
            sec_dict[p_key][j] = tmp_sec.getElementAsString(j)

    session.stop()

    results = pd.DataFrame.from_dict(sec_dict, orient='index')
    return results


def security_lookup(query, yellowKeyFilter="YK_FILTER_INDX", languageOverride="LANG_OVERRIDE_ENGLISH", maxResults=100, print_request=False):
    """
    The Security Lookup (a.k.a. Instrument Lookup) Request constructs a search based upon the "query" Element's string value, as well as the additional filters such as the yellow key and language override Elements. This functionality can also be found on the Bloomberg Professional service using the SECF <GO> function. By setting the language override Element, users get results translated into the specified language.
    
    yellowKeyFilters:
        YK_FILTER_NONE  
        YK_FILTER_CMDT  
        YK_FILTER_EQTY  
        YK_FILTER_MUNI  
        YK_FILTER_PRFD  
        YK_FILTER_CLNT  
        YK_FILTER_MMKT  
        YK_FILTER_GOVT  
        YK_FILTER_CORP  
        YK_FILTER_INDX  
        YK_FILTER_CURR  
        YK_FILTER_MTGE  
    """
    
    # these are already the default so you don't really need to do this
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)

    # starts the session
    session = blpapi.Session(sessionOptions)
    if (not session.start()):
        print "Failed to start session"

    # opens the service for the session
    if (not session.openService("//blp/instruments")):
        print "failed to open //blp/instruments service"

    # get the service
    secfService = session.getService("//blp/instruments")
    request = secfService.createRequest("instrumentListRequest")

    

    request.set("query", query)
    request.set("yellowKeyFilter", yellowKeyFilter)
    request.set("languageOverride", languageOverride) # or LANG_OVERRIDE_NONE
    request.set("maxResults", maxResults)
    
    if print_request:
        print request

    test_event = easyRequest(session, request)
    del request
    
    RESULTS_DATA = blpapi.Name("results")

    securityData = blpapi.event.MessageIterator(test_event).next().getElement(RESULTS_DATA)

    sec_dict = dict()
    for i in xrange(securityData.numValues()):
        tmp_sec = securityData.getValueAsElement(i)
        sec_dict[tmp_sec.getElementAsString('security')] = tmp_sec.getElementAsString('description')

    session.stop()

    results = pd.DataFrame.from_dict(sec_dict, orient='index')
    results.columns = ['description']
    results.index = [x.split('<')[0] for x in results.index.values]
    return results


def curve_lookup(query, bbgid='', countryCode='', currencyCode='', curveid='', curve_type=None, subtype=None, maxResults=100, print_request=False):
    """
    The Curve Lookup Request can retrieve a Curve based on its country code, currency code, type, subtype, Curve-specific ID and the Bloomberg ID for that Curve
    """
    
    # these are already the default so you don't really need to do this
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)

    # starts the session
    session = blpapi.Session(sessionOptions)
    if (not session.start()):
        print "Failed to start session"

    # opens the service for the session
    if (not session.openService("//blp/instruments")):
        print "failed to open //blp/instruments service"
    
    # get the service
    secfService = session.getService("//blp/instruments")
    request = secfService.createRequest("curveListRequest")
    
    # You can search by some of the other request elements
    request.set("query", query)
    request.set("bbgid", bbgid)
    request.set("countryCode", countryCode)
    request.set("currencyCode", currencyCode)
    request.set("curveid", curveid)
    request.set("maxResults", maxResults)
    # for whatever reason these need to be set conditionally
    if curve_type is not None:
        request.set("type", curve_type)
    if subtype is not None:
        request.set("subtype", subtype)
    
    if print_request:
        print request
    
    test_event = easyRequest(session, request)
    del request
    
    RESULTS_DATA = blpapi.Name("results")

    securityData = blpapi.event.MessageIterator(test_event).next().getElement(RESULTS_DATA)
    sec_dict = dict()
    for i in xrange(securityData.numValues()):
        tmp_sec = securityData.getValueAsElement(i)
        curve_id = tmp_sec.getElementAsString('curve')
        sec_dict[curve_id] = dict()
        for j in ['description', 'country', 'currency', 
                  'curveid', 'publisher', 'bbgid']:
            sec_dict[curve_id][j] = tmp_sec.getElementAsString(j)
        for k in ['type', 'subtype']:
            data_list = list()
            for l in xrange(tmp_sec.getElement(k).numValues()):
                data_list.append(tmp_sec.getElement(k).getValueAsString(l))
            sec_dict[curve_id][k] = data_list

    session.stop()

    results = pd.DataFrame.from_dict(sec_dict, orient='index')
    return results

def portfolio_data_request(portfolio_id, field='PORTFOLIO_MWEIGHT', ref_date=None):
    import pandas as pd
    import blpapi

    # these are already the default so you don't really need to do this
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)

    # starts the session
    session = blpapi.Session(sessionOptions)
    if (not session.start()):
        print "Failed to start session"

    # opens the service for the session
    if (not session.openService("//blp/refdata")):
        print "failed to open //blp/refdata service"

    # get the service
    secfService = session.getService("//blp/refdata")
    request = secfService.createRequest("PortfolioDataRequest")
    
    # create the request
    securities = request.getElement("securities")
    securities.appendValue(portfolio_id)
    
    pfields = request.getElement("fields")
    pfields.appendValue(field)

    # overrides
    # reference date
    if ref_date is not None:
        overrides = request.getElement("overrides")
        overrides1 = overrides.appendElement()

        overrides1.setElement("fieldId", "REFERENCE_DATE")
        overrides1.setElement("value", ref_date)
    
    # this is really messy
    # I needed to redefine this within the scope of the function to accomodate a change in the secInfoWindow variable
    def PeasyRequest(sess, req):
        """
        sess : Session
        req : Request
        """
        class Window(object):
            def __init__(self, name):
                self.name = name

            def displaySecurityInfo(self, msg):
                print "%s: %s" % (self.name, msg)
        
        eventQueue = blpapi.event.EventQueue()
        # this part is different
        secInfoWindow = Window("securityData")
        cid = blpapi.CorrelationId(secInfoWindow)
        sess.sendRequest(req, correlationId = cid, eventQueue = eventQueue)
        while(True):
            eventObj = eventQueue.nextEvent()
            if eventObj.eventType() == blpapi.event.Event.RESPONSE:
                # A RESPONSE Message indicates the request has been fully served
                break
        return eventObj
    
    test_event = PeasyRequest(session, request)
    RESULTS_DATA = blpapi.Name("securityData")

    securityData = blpapi.event.MessageIterator(test_event).next().getElement(RESULTS_DATA)
    secdata2 = securityData.getValue(0)

    fld_data = secdata2.getElement("fieldData")
    
    positions = fld_data.getElement(field)

    port_dict = {}
    
    for i in xrange(positions.numValues()):
        pos = positions.getValue(i)
        sec_name = pos.getElementAsString("Security")
        sec_weight = pos.getElementAsString("Weight")
        port_dict[sec_name] = sec_weight
    
    my_portfolio = pd.DataFrame.from_dict(port_dict, orient='index')[0].rename('weight').astype(float)
    
    session.stop()
    return my_portfolio