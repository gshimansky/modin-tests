import numpy as np
import pandas as pd
#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk2/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)
#import modin.pandas as pd

df = pd.DataFrame([[1, 1, 10],
                   [2, 4, 20],
                   [3, 7, 30]],
                  index=[1, 2, 3],
                  columns=['id', 'max_speed', 'health'])
print(df)

df1 = df.groupby(['id', 'health']).agg({'max_speed': 'max'})
#df1 = df.groupby(['id', 'health'], as_index=False).agg({'max_speed': 'max'})
print(df1)
