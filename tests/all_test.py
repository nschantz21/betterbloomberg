from betterbloomberg import get

data = get(
    kind="ReferenceDataRequest",
    securities=["AAPL US EQUITY",],
    fields=["PX_LAST",]
)

print(data)
