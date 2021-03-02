import pandas as pd

df = pd.DataFrame({"col": [1, 1, 2, 2, 1, 1, 2, 2,],}, index=[1, 1, 0, 0, 1, 1, 0, 0,],)
print(df)
df1 = df.sort_values(by=["col"])
print(df1)
