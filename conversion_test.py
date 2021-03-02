#import pandas as pd
import modin.pandas as pd

col = pd.Series([1, 2, 3])
print(type(col[0]))
print(type(col[1]))
col[0] = 1.1
print(type(col[0]))
print(type(col[1]))
