import numpy as np
#import pandas as pd
import ray
ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)
import modin.pandas as pd

df = pd.DataFrame([[2, np.datetime64('2013-08-01 08:14:37'), 1.1],
                   [5, np.datetime64('2014-08-01 09:13:00'), 2.2],
                   [8, np.datetime64('2015-08-01 09:48:00'), 3.3]],
                  index=[1, 2, 3],
                  columns=['health', 'timestamp', 'shield'])
print(df)

transformed = df[['health', 'timestamp', 'shield']].transform({
    'health': lambda x: x,
    'timestamp': lambda x: pd.DatetimeIndex(x).year,
    'shield': lambda x: x}).groupby(['health', 'timestamp', 'shield'])
print(transformed)

df1 = transformed.size().reset_index().sort_values(by=['timestamp',0],ascending=[True,False])
print(df1)
