import pandas as pd

df = pd.DataFrame({"col": [2, 1, 1],})
print(df)
print(df.index)
df1 = df.sort_values(by=["col"])
print(df1)
print(df1.index)
print("----------------------------")
df.sort_values(by=["col"], inplace=True)
print(df)
print(df.index)
