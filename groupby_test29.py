import modin.pandas as pd

df = pd.DataFrame({
    "col0": [ 1,  2,  1,  2],
    "col1": [11, 12, 13, 14],
    "col2": [ 4,  4,  4,  4],
    "col3": [ 1,  1,  1,  1]
})

print(df.shape)
gb = df.groupby("col0")
result = gb.agg({"col0": "count"})
string = "%s" % repr(result)
print(string)
