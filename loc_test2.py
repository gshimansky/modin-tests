#import pandas as pd
import ray
ray.init(huge_pages=True, plasma_directory="/mnt/hugepages")
import modin.pandas as pd

df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
                  index=[1, 2, 3],
                  columns=['max_speed', 'shield'])
print(df)

condition = df['shield'] > 6
print(condition)

df.loc[condition, 'new_col'] = df.loc[condition, 'max_speed']
print(df)
