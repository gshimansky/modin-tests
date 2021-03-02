import pandas as pd

df = pd.DataFrame(columns=["col1", "col2"])

df.iloc[1] = [11, 22]
print(df)
