import modin.pandas as pd
import numpy as np

random_state = np.random.RandomState(seed=42)


df = pd.DataFrame({"col1": [1, 10], "col2": [20, 5]})
data1 = {}
d1 = []
d2 = []
d1.append(1)
d1.append(10)
d2.append(20)
d2.append(5)
data1["col1"] = d1
data1["col2"] = d2
df1 = pd.DataFrame(data1)
data2 = {
    "col{}".format(int((i - 2 / 2) % 2 + 1)): random_state.randint(0, 10, size=(2))
    for i in range(2)
}
print(type(data2["col1"]))
print(data2["col1"].flags)
print(data2["col1"])
df2 = pd.DataFrame(data2)
print(df2.rank())
