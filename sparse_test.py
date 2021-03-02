import modin.pandas as pd

df = pd.DataFrame(
    {
        "col1": [0, 1, 2, 3],
        "col2": [4, 5, 6, 7],
    }
)

#print("df.sparse = ", df.sparse)

if not hasattr(df, 'sparse'):
    print("No sparse")
else:
    print("sparse")
