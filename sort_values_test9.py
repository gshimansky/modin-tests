import pandas as pd

def do_nothing(value):
    return value

df = pd.DataFrame(
    {
        "col1": [2, 51, 17],
        "col2": [5.5, 3.3, 4.4],
        "col3": ["a", "b", "c"],
    },
)
print(df)
# Pandas bug #41318
df1 = df.sort_values(by=["col1"], ascending=None, key=do_nothing)
print(df1)
