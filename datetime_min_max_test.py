import numpy as np
import pandas as pd

df = pd.DataFrame([[1, 2.2, np.datetime64('2013-08-01 08:14:37'), "aaa"],
                   [4, 5.5, np.datetime64('2014-08-01 09:13:00'), "bbb"],
                   [7, 8.8, np.datetime64('2015-08-01 09:48:00'), "ccc"]],
                  index=[1, 2, 3],
                  columns=['int', 'float', 'timestamp', 'string'])
print(df)
print(df.dtypes)

print(df.min())
print(df.max())
