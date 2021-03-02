import modin.pandas as pd
import numpy as np

index_tuples = [
    ("bar", "one"),
    ("bar", "two"),
    ("baz", "one"),
    ("baz", "two"),
]

pandas_index = pd.MultiIndex.from_tuples(
    index_tuples, names=["first", None])
frame_data = np.random.randint(0, 100, size=(4, 4))
df = pd.DataFrame(
    frame_data,
    index=pandas_index,
)
print(df)
df1 = df.reset_index(drop=False)
print(df1)
