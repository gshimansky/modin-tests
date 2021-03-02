import modin.pandas as pd

df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
                  index=[1, 2, 3],
                  columns=['max_speed', 'shield'])
print(df)

gb = df.groupby('max_speed')
print("type(gb) = ", type(gb))

col1 = gb['max_speed']
print("type(col1) = ", type(col1))
df1 = col1.count()
print(type(df1))
print(df1)

col2 = gb[['max_speed']]
print("type(col2) = ", type(col2))
df2 = col2.count()
print(type(df2))
print(df2)
