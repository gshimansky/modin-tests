import numpy as np
#import pandas as pd
import ray
import modin.pandas as pd

df = pd.DataFrame([[2, np.datetime64('2017-01-01 00:00:00'), 1.1],
                   [5, np.datetime64('2017-01-01 00:01:00'), 2.2],
                   [8, np.datetime64('2017-01-01 00:02:00'), 3.3],
                   [8, np.datetime64('2017-01-01 00:03:00'), 4.4],
                   [8, np.datetime64('2017-01-01 00:04:00'), 5.5]],
                  columns=['health', 'time', 'shield'])
print(df)
print(df['time'].head())
