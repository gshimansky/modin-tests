if __name__ == "__main__":
    #import pandas as pd
    import modin.pandas as pd

    series_length = 10_000
    df = pd.DataFrame(
        {
            "col1": ["str{0}".format(i) for i in range(0, series_length)],
            "col2": [i for i in range(0, series_length)],
        }
    )

    df1 = df.astype({"col1": "category"})
    print(df1)
    print(df1.dtypes)
    print(type(df1["col1"].values))
    print(df1["col1"].values)
