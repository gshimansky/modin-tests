import numpy as np
import modin.pandas as pd
#import pandas as pd

df = pd.DataFrame({
    "col1": [ 1,  2,  1,  2,  1,  3,  3,  2,  3],
    "col2": [11, 12, 13, 14, 15, 16, 17, 18, 19],
    "col3": [ 4,  4,  4,  4,  5,  5,  5,  5,  5],
    "col4": [ 1,  1,  1,  1,  1,  1,  1,  1,  1]
})
print(df, '\n')

df0 = df.agg({5: 'max'}, axis=1)
print(df0, '\n')
df3 = df.agg({'col1': 'max', 'col2': 'sum'})
print(df3, '\n')
df4 = df.agg({'col1': 'max', 'col2': 'min', 'col3': 'sum'})
print(df4, '\n')
df4 = df.agg({'col1': 'max', 'col2': 'min', 'col3': 'sum', 'col4': 'mean'})
print(df4, '\n')
