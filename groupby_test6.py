import numpy as np
#import pandas as pd
import modin.pandas as pd

df = pd.DataFrame([[1, 1, 10],
                   [2, 4, 20],
                   [3, 7, 30]],
                  index=[1, 2, 3],
                  columns=['id', 'max_speed', 'health'])
print(df)

df1 = df.groupby(['id', 'health']).max()
#df1 = df.groupby(['id', 'health'], as_index=False).sum()
print(df1)
