import pandas as pd
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
by = "col3"
transform_function = lambda df: df + 4

gb = df.groupby(by, as_index=True)

df1 = gb.transform(transform_function)
print(df1)
print(df1.shape)
