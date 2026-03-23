import pandas as pd

df = pd.read_parquet("data/chunk_200001_210000.parquet")

print(df.columns)      # список колонок
print(df.head(3))      # первые строки
print(df.dtypes)       # типы