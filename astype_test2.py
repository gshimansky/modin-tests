import modin.pandas as pd

df = pd.DataFrame(index=["row1", "row2", "row3"], columns=["col1"])
df.loc["row1"]["col1"] = 11
df.loc["row2"]["col1"] = 21
df.loc["row3"]["col1"] = 31
print(df)
print(df.dtypes)
df1 = df.astype(int)
print(df1)
print(df1.dtypes)
