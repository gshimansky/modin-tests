import numpy as np
import pandas as pd
#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)
#import modin.pandas as pd

df = pd.DataFrame([[1, 2, np.datetime64('2013-08-01 08:14:37')],
                   [4, 5, np.datetime64('2014-08-01 09:13:00')],
                   [7, 8, np.datetime64('2015-08-01 09:48:00')]],
                  index=[1, 2, 3],
                  columns=['max_speed', 'shield', 'timestamp'])
print(df)

transformed = df[['max_speed', 'timestamp']].transform({'max_speed': lambda x: x, 'timestamp': lambda x: pd.DatetimeIndex(x).year})
print(transformed)

df1 = transformed.groupby(['max_speed', 'timestamp'])[['max_speed', 'timestamp']].count()['max_speed']
print(df1)
