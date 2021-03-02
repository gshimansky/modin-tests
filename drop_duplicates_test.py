#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman/plasma", memory=1024*1024*1024*200, object_store_memory=1024*1024*1024*200)

if __name__ == "__main__":
    import pytest
    import inspect
    import modin.pandas as pd
    #import pandas as pd
    df = pd.DataFrame([["one", 1, 10],
                       ["two", 4, 20],
                       ["two", 5, 20],
                       ["three", 7, 30]],
                      index=[1, 2, 3, 4],
                      columns=['name1', 'max_speed', 'health'])
    print(df)

    def test_func(df, arg):
        print("ARG=", arg)
        df1 = df.drop_duplicates(arg)
        print(df1)

    test_func(df, ["name1"])
    test_func(df, ("name1"))
    test_func(df, "name1")
    test_func(df, ["name1", "health"])
    test_func(df, ("name1", "health"))
    test_func(df, ["name1", "max_speed"])
    test_func(df, ("name1", "max_speed"))
