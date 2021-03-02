from typing import Any, Union

import numpy as np
#import pandas as pd
#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk2/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)
import modin.pandas as pd
from modin.pandas import DataFrame

df = pd.DataFrame([[1, 1, 2, 10],
                   [2, 4, 5, 20],
                   [3, 7, 8, 30]],
                  index=[1, 2, 3],
                  columns=['id', 'max_speed', 'shield', 'health'])
print(df)

df1: Union[DataFrame, Any] = df.groupby(['id', 'health'], as_index=False).agg({'max_speed': 'max', 'shield': 'max'})
print(df1)
