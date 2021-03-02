import modin.pandas as pd
import numpy as np

df = pd.DataFrame(
    {
        "col1": [0, 1, 2, 3],
        "col2": [4, 5, np.NaN, 7],
        "col3": [np.NaN, np.NaN, 12, 10],
        "col4": [17, 13, 16, 15],
        "col5": [-4, -5, -6, -7],
    }
)
#df = df.astype({"col1": "category"})
print(df)
#by=[pd.Series([1, 5, 7, 8])]
by=["col1", pd.Series([1, 5, 7, 8])]
gb = df.groupby(by)
result = gb.shift(periods=0)
print(result)
