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
by = "col1"
apply_function = lambda df: df.sum()

gb = df.groupby(by, as_index=True)

df1 = gb.apply(apply_function)
print(df1)
print(df1.shape)
