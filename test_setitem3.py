import modin.pandas as pd

df = pd.DataFrame({"id": [1], "max_speed": [2], "health": [3]})
se = pd.Series([11, 22, 33])
df[0] = se
print(df)
