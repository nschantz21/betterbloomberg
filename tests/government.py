from betterbloomberg.search import Government

result = Government("T", partial_match=True, ticker="TI")
print(result.data)
