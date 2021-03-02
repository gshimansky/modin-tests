import modin.pandas as pd
import numpy as np

df2 = pd.DataFrame({"col1": [11, 21, 31, 41], "col2": [12, 22, 32, 42]})
values = np.array([[1], [2], [3], [4]])
print(type(values))
print(type(values[0]))
print(values)
df2['data'] = values
print(df2)
s1 = df2['data'].unique()
print(s1)
