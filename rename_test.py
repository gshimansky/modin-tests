import pandas as pd
#import modin.pandas as pd

df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
                  index=[1, 2, 3],
                  columns=['index', 'shield'])
df.reset_index()
print(df)
print(df.columns)
df1 = df.rename({'index': 'name'}, axis='columns')
print(df1)
print(df1.columns)

