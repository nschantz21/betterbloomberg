from betterbloomberg.reference_data import ReferenceDataRequest as ref

TICKER = "AAPL US EQUITY"
FLD = "BEST_ANALYST_RECS_BULK"

overrides = {"END_DATE_OVERRIDE":"20200430"}

req = ref(TICKER, FLD)
data = req.get_data()
