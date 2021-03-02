import modin.pandas as pd

df = pd.DataFrame()
se = pd.Series([11, 22, 33])
df[0] = se
print(df)
