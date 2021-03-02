import modin.pandas as pd

df = pd.DataFrame({"col": [2, 51, 17],}, index=["abc", "def", "ghi"])
print(df)
print(df.index)
df.sort_values(by=["col"], inplace=True)
print(df)

