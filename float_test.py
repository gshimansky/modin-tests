if __name__ == "__main__":
    import numpy as np
    import modin.pandas as pd

    df = pd.DataFrame(np.arange(1, 100000, dtype=np.float32), columns=["s1"])
    s2 = np.power(df['s1'], np.float64(2.0))
    df['s2'] = s2
    print(df['s2'][12724])
    s3 = df['s1'] * df['s2']

