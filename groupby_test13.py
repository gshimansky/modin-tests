import modin.pandas as pd

df = pd.DataFrame(
    {
        "col1": [0, 1, 2, 3],
        "col4": [17, 13, 16, 15],
        "col5": [-4, -5, -6, -7],
    }
)
by=["col4", "col5"]
apply_function = min

gb = df.groupby(by, as_index=True)

df1 = gb.apply(apply_function)
print(df1)

df2 = gb.min()
print(df2)

df3 = gb.apply(apply_function)
print(df3)
