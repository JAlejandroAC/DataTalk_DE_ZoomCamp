import sys
import pandas as pd

month = int(sys.argv[1])

df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
print(df.head())


df.to_parquet(f'output_{month}.parquet')

print(f'hello pipeline, month = {month}')