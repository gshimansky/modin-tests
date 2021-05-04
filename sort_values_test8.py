import pandas as pd

df = pd.DataFrame({"col1": [2, 51, 17], "col2": [5.5, 3.3, 4.4], "col3": ["a", "b", "c"]}, index=["abc", "def", "ghi"])
print(df)
print(df.index)
df.sort_values(by=["col1"], inplace=True, ignore_index=True, na_position="last")
print(df)

