import modin.pandas as pd

df = pd.DataFrame({"id": [4, 40, 400], "max_speed": [111, 222, 333], "health": [33, 22, 11]})
se = pd.Series([11, 22])
df['id'] = se
print(df)
