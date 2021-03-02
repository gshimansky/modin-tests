import modin.pandas as pd
import numpy as np

df = pd.DataFrame(
    {
        "col1": [0, 1, 2, 3],
        "col2": [4, 5, 6, 7],
        "col3": [3, 8, 12, 10],
        "col4": [17, 13, 16, 15],
        "col5": [-4, -5, -6, -7],
    }
)
by = "col1"
gb = df.groupby(by, as_index=False)
df1 = gb.prod()
print(df1)

df = pd.DataFrame(
    {
        "col1": [0, 1, 2, 3],
        "col2": [4, 5, np.NaN, 7],
        "col3": [np.NaN, np.NaN, 12, 10],
        "col4": [17, 13, 16, 15],
        "col5": [-4, -5, -6, -7],
    }
)

gb = df.groupby(by, as_index=False)
df1 = gb.prod()
print(df1)
