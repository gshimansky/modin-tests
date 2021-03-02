if __name__ == "__main__":
    import numpy as np
    import pandas as pd

    s1 = pd.Series([1, 2, 3, 4, 5], name = "s1")
    nparray = np.arange(1, len(s1) + 1)
    s2 = s1 / nparray
    print("s2.name = ", s2.name)
    s3 = s1.div(nparray)
    print("s3.name = ", s3.name)

    numbers_list = [1.0, 2.0, 3.0, 4.0, 5.0]
    s4 = s1 / numbers_list
    print("s4.name = ", s4.name)
    s5 = s1.div(numbers_list)
    print("s5.name = ", s5.name)

    numbers_tuple = (1.0, 2.0, 3.0, 4.0, 5.0)
    s6 = s1 / numbers_tuple
    print("s6.name = ", s6.name)
    s7 = s1.div(numbers_tuple)
    print("s7.name = ", s7.name)

    pd.show_versions()
