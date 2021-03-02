import numpy as np
#import modin.pandas as pd
import pandas as pd

df = pd.DataFrame({
    "col1": [ 1,  2,  1,  2,  1,  3,  3,  2,  3],
    "col2": [11, 12, 13, 14, 15, 16, 17, 18, 19],
    "col3": [ 4,  4,  4,  4,  5,  5,  5,  5,  5],
    "col4": [ 1,  1,  1,  1,  1,  1,  1,  1,  1]
})
print(df)

gb0 = df.groupby('col3')
df0 = gb0.agg({'col2': 'max'})
print(df0)
gb1 = df.groupby('col3')
df1 = gb1.agg(new_col=('col2', max))
print(df1)
gb2 = df.groupby(['col1', 'col3'])
df2 = gb2.agg({'col2': 'max'})
print(df2)
gb3 = df.groupby(['col1', 'col3'])
df3 = gb3.agg({'col2': 'max', 'col4': 'sum'})
print(df3)
gb4 = df.groupby(['col1', 'col3'])
df4 = gb4.agg(new_col1=('col2', 'max'), new_col2=('col2', 'min'), new_col3=('col2', 'sum'),
              new_col4=('col4', 'max'), new_col5=('col4', 'min'), new_col6=('col4', 'sum'))
print(df4)
