if __name__ == "__main__":
    import modin.pandas as pd

    df1 = pd.read_csv("x.csv")
    df2 = pd.DataFrame({"name": [39], "position": [0]})
    df3 = df1.merge(df2, on='name', how='inner')
    print(df3)
