import pandas
import numpy as np

frame_data = {
    "a": [np.nan, 1, 2, np.nan, np.nan],
    "b": [1, 2, 3, np.nan, np.nan],
    "c": [np.nan, 1, 2, 3, 4],
}
df = pandas.DataFrame(frame_data, index=list("VWXYZ"))

s1 = pandas.Series([np.nan, 10, 20, 30, 40], index=["a", "c", "x", "y", "z"])
