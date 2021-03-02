import modin.pandas as pd

df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                  index=[1, 2, 3],
                  columns=['id', 'max_speed', 'shield'])
print(df)

df1 = df.groupby(['max_speed', 'shield']).size()
print(df1)
print(df1.axes)
