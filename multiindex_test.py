from modin import pandas as pd
df = pd.DataFrame(
     [["bar", 1], ["bar", 2], ["foo", 1], ["foo", 2]],
     columns=["first", "second"],
)
print(type(df))
mi = pd.MultiIndex.from_frame(df._to_pandas())
print(mi)
