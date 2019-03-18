import pandas as pd

products = pd.read_csv('../data/products.csv')

p = products[['product_code','product_description']]

p = p.dropna()

p.product_code = p.product_code.astype(int)

p = p.drop_duplicates()

p.to_csv('../data/productDesc.csv', index=False)