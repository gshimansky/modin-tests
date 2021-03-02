import pandas as pd

s1 = pd.Series(['AAA', 'BBB', 'CCC'], dtype="category")
print(s1)
s2 = pd.concat([s1, s1])
print(s2)
df = pd.DataFrame([[1, 'AAA'], [4, 'BBB'], [7, 'CCC']],
                  index=[1, 2, 3],
                  columns=['max_speed', 'shield'])
df['cat'] = df['shield'].astype("category")
print(df)
print(df['cat'])
df2 = pd.concat([df, df])
print(df2)
print(df2['cat'])
