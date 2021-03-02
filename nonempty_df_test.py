import numpy as np
import pandas as pd
#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)
#import modin.pandas as pd

df = pd.DataFrame([[1, 1, 10], [2, 4, 20]], columns=['id', 'max_speed', 'health'])
print(df)

a = np.array(['one'])
print(type(a))
df['id'] = a
print(df)
