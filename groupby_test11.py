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
print(df)
by=["col1", "col2"]
#print(df["col1"].hasnans)
#print(df["col2"].hasnans)
#print(any(df[x].hasnans for x in by))

gb = df.groupby(by)

print("groups = ", gb.groups)
print("len(groups) = ", len(gb.groups))
print("indices = ", gb.indices)

for col, frame in gb:
    print("Frame for col", col)
    print(frame)

df1 = len(gb)
print(df1)
