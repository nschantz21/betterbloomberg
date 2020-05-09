from betterbloomberg.study import Study

result = Study(
    "IBM US EQUITY",
    "dmi",
    "20200101",
    "20200201",
    priceSourceLow="PX_LOW",
    priceSourceClose="PX_LAST",
    priceSourceHigh="PX_HIGH"
)

print(result.data)

result2 = Study(
    "IBM US EQUITY",
    "macd",
    "20200219",
    "20200501",
    priceSourceClose="PX_LAST",
)
print(result2.data)