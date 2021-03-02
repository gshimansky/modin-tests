import numpy as np
#import pandas as pd
import modin.pandas as pd

df = pd.DataFrame()
print(df)

df['id'] = pd.Series([1, 2])
print(df)
