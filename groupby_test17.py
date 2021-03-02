import modin.pandas as pd
import numpy as np

df = pd.DataFrame(
    {
        "col1": ["a", "a", "a", "b", "b", "c", "c", "c"],
        "col2": [10, 20, 30, 40, 50, 60, 70, 80],
    })

def func(data):
    return 111.11

apply_func = lambda df: 111.11

print(df)
gb = df.groupby("col1")
print(gb.groups)
for col, frame in gb:
    print("Frame for col", col)
    print(frame)

df1 = gb.apply(func)
print(df1)
print(df1.shape)
