from betterbloomberg.reference_data import HistoricalDataRequest

req = HistoricalDataRequest(
    ["IBM US EQUITY", "MSFT US EQUITY"],
    ["PX_LAST", "OPEN"],
    "20200402"
)
print(req.data)
