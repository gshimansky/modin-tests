#import pandas as pd
import ray
ray.init(huge_pages=True, plasma_directory="/mnt/hugepages")
import modin.pandas as pd

df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
                  index=[1, 2, 3],
                  columns=['max_speed', 'shield'])
print(df)

df1 = df[['max_speed', 'shield']]
print(df1)

df2 = df1.groupby('max_speed', as_index=False).max()
print(df2)
