from betterbloomberg.field import FieldSearch

result = FieldSearch("EPS")
print(result.data)
result.data.to_csv("field_search.csv")
