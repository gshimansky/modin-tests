import modin.pandas as pd

df1 = pd.DataFrame(index=["row1"], columns=["col1"])
df1.loc["row1"]["col1"] = 11
print(df1)
