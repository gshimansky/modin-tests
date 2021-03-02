if __name__ == "__main__":
    #import pandas as pd
    import modin.pandas as pd

    df = pd.DataFrame([[1, 1, 10],
                       [2, 4, 20],
                       [3, 7, 30]],
                      index=[1, 2, 3],
                      columns=['id', 'max_speed', 'health'])

    df1 = pd.DataFrame(df['health'], columns=['sum'])
    print(df1)
