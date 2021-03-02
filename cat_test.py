if __name__ == "__main__":
    #import pandas as pd
    import modin.pandas as pd
    df1 = pd.read_csv("categories.json", dtype={"one": "int64", "two": "category"})

    df = pd.read_csv("categories.csv", names=["one", "two"], dtype={"one": "int64", "two": "category"})

    print(df.dtypes.describe())
    print("type(df.dtypes[1]) = ", type(df.dtypes[1]))
    print("df.dtypes[1] = ", df.dtypes[1])
    print("type(df.dtypes[1].categories) = ", type(df.dtypes[1].categories))
    print("df.dtypes.categories = ", df.dtypes[1].categories)
    s = df["two"]
    #print("s.describe = ", s.describe())
    print("type(s.dtypes.categories) = ", type(s.dtypes.categories))
    print("s.dtypes.categories = ", s.dtypes.categories)
    print("type(s.dtypes) = ", type(s.dtypes))
    print("s.dtypes = ", s.dtypes)
    print(s)