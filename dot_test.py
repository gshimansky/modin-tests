import modin.pandas as pd
import numpy as np

df = pd.DataFrame({
    "A": [11, 21, 31, 41],
    "B": [12, 22, 32, 42]
})
df2 = pd.DataFrame({"A": [1.0, 0.0], "B": [0.0, 1.0]})

print(df)
print(df.index)
df.index.name = "name"
df.index = df.index
print(df.index)
if hasattr(df, "_query_compiler"):
    print(df._query_compiler._modin_frame._partitions[0][0].data.index)
print(df2)
print(df2.index)
df3 = df2.head(2)
print(df3)
print(df3.index)
df4 = df.dot(df3.T)
print(df4)
print(df4.index)
