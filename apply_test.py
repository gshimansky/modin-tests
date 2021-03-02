import modin.pandas as pd
import numpy as np

df = pd.DataFrame(
    {
        "col1": ["a", "a", "a", "b", "b", "c", "c", "c"],
        "col2": [10, 20, 30, 40, 50, 60, 70, 80],
    })

print(df)
df1 = df.apply(lambda s: pd.Series([111.11, 222.22], index=['aa', 'bb']))
print(df1)
print(df1.shape)
