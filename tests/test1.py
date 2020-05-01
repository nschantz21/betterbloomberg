from betterbloomberg.reference_data import ReferenceDataRequest as ref
import pandas as pd

req = ref("AAPL US EQUITY", "PX_LAST")
data = req.get_data()
print(data)

data = ref(["AAPL US EQUITY", "IBM US EQUITY"], ["PX_LAST", "PX_OPEN"]).get_data()
print(pd.DataFrame(data))

