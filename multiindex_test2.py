import numpy as np

def example():
    index_tuples = [
        ("bar", "one"),
        ("bar", "two"),
        ("bar", "three"),
        ("bar", "four"),
        ("baz", "one"),
        ("baz", "two"),
        ("baz", "three"),
        ("baz", "four"),
        ("foo", "one"),
        ("foo", "two"),
        ("foo", "three"),
        ("foo", "four"),
        ("qux", "one"),
        ("qux", "two"),
        ("qux", "three"),
        ("qux", "four"),
    ]

    column_tuples = [
        (1.1, 1),
        (1.1, 2),
        (1.1, 3),
        (1.1, 4),
        (2.2, 1),
        (2.2, 2),
        (2.2, 3),
        (2.2, 4),
        (3.3, 1),
        (3.3, 2),
        (3.3, 3),
        (3.3, 4),
        (4.4, 1),
        (4.4, 2),
        (4.4, 3),
        (4.4, 4)
    ]

    pandas_index = pd.MultiIndex.from_tuples(
        index_tuples, names=["first", "second"])
    pandas_columns = pd.MultiIndex.from_tuples(
        column_tuples, names=["1st", "2nd"])
    frame_data = np.random.randint(0, 100, size=(16, 16))
    df = pd.DataFrame(
        frame_data,
        index=pandas_index,
        columns=["col{}".format(i) for i in range(16)],
    )
    df.columns = pandas_columns
    print(df)
    df1 = df.loc[("bar"), "two", slice(None), slice(None), :]
    print(df1)

import pandas as pd
example()
#                data
# first  second     
# bar    1       1
#        2       2
from modin import pandas as pd
example()
# ...
# IndexingError: Too many indexers
