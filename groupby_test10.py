import modin.pandas as pd

df = pd.DataFrame([[2, 2, 3], [6, 5, 6], [7, 8, 9]],
                  index=[1, 2, 3],
                  columns=['id', 'max_speed', 'shield'])
print(df)

ai = False
gb = df.groupby(['max_speed'], as_index=ai)
print("as_index = ", ai)
df1 = gb.all()
print("df1.columns = ", df1.columns)

print(df1)
print(df1.axes)
