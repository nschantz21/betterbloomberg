from betterbloomberg.reference_data import ReferenceDataRequest as ref
import pandas as pd

TICKER = "AAPL US EQUITY"
FLD = "BEST_ANALYST_RECS_BULK"

overrides = {"END_DATE_OVERRIDE":"20200430"}

req = ref(TICKER, FLD)
data = req.get_data()
print(data)
