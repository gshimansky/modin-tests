import pandas as pd
import modin.pandas as mpd

pdf = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
                   index=[0, 1, 2],
                   columns=['index', 'shield'])
mdf = mpd.DataFrame([[1, 2], [4, 5], [7, 8]],
                    index=[0, 1, 2],
                    columns=['index', 'shield'])

print(pdf['index'][0])
print(mdf['index'][0])

pdf['index'][0] = 1000
mdf['index'][0] = 1000

print(pdf['index'][0])
print(mdf['index'][0])
