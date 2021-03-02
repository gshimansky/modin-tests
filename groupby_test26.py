import modin.pandas as pd
#import pandas as pd

df = pd.DataFrame({
    "col1": [ 1,  2,  1,  2,  1,  3,  3,  2,  3],
    "col2": [11, 12, 13, 14, 15, 16, 17, 18, 19],
    "col3": [ 4,  4,  4,  4,  5,  5,  5,  5,  5],
    "col4": [ 1,  1,  1,  1,  1,  1,  1,  1,  1]
})
print(df)

gb0 = df.groupby('col3', as_index=False)
df0 = gb0.agg(sum)
print("agg func:\n", df0)

gb1 = df.groupby('col3', as_index=False)
df1 = gb1.agg('sum')
print("agg string:\n", df1)

gb2 = df.groupby('col3', as_index=False)
df2 = gb2.apply(sum)
print("apply:\n", df2)

gb3 = df.groupby('col3', as_index=False)
df3 = gb3.agg(lambda df: df.sum())
print("agg lambda func:\n", df3)

gb4 = df.groupby('col3', as_index=False)
df4 = gb4.apply(lambda df: df.sum())
print("apply lambda:\n", df4)
