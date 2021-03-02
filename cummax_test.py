import modin.pandas as pd

data={'col1': [1, 2], 'col2': [3.3, 4.4]}
df = pd.DataFrame(data)
df1 = df.cummax(axis=1)
print(df1)

