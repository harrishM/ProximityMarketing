import pandas as pd
import numpy as np
import graphlab
from graphlab import SArray


products = pd.read_csv('../data/products.csv')
products.drop(['DOB','Gender','State','PinCode','store_description','till_no','transaction_number_by_till','promo_code','promotion_description','product_description','sale_price_after_promo','discountUsed'],axis=1, inplace=True)
sample = pd.read_csv('../data/sampleSubmission.csv')
sample.head()
products['customerID'].nunique()

products['transactionDate'] = pd.to_datetime(products['transactionDate'])
products.sort_values('transactionDate',inplace=True)
#mask = (products['transactionDate'] >= '2016-12-01')
mask_2015 = (products['transactionDate'] >= '2015-07-01') & (products['transactionDate'] <= '2015-07-31')
mask_2016 = (products['transactionDate'] >= '2016-07-01') & (products['transactionDate'] <= '2016-07-31')
mask_2017 = (products['transactionDate'] >= '2017-01-01') & (products['transactionDate'] <= '2017-06-30')
products_2016 = products.loc[mask_2016]
products_2017 = products.loc[mask_2017]
products_2015 = products.loc[mask_2015]
pds = pd.concat([products_2016, products_2015, products_2017])
#pd
#products = products.loc[mask]

products = products.reset_index(drop=True)


products = pds[['customerID', 'transactionDate', 'product_code','store_code']]
## take only those customers which are in sample submission file
products_2 = products[products['customerID'].isin(sample['customerID'])]

## remove missing values # 4
products_2 = products_2[~pd.isnull(products_2['product_code'])]

## convert type of product code
products_2['product_code'] = products_2['product_code'].astype(np.int64)

products_2 = products_2.loc[:,['customerID','product_code','store_code']]
products_2 = products_2.reset_index(drop=True)

## these customers are not in train, so we'll cant predictso we keep same as in the sample submission
misfit_customers = list(set(sample['customerID']) - set(products_2['customerID']))


## create rating column , which create recommendation of top product by counting number time customer like and brought
products_2['rating'] = products_2.groupby('product_code')['product_code'].transform('count')

# Removing rare products, this could be further redcued by keepinng only top products
products_2 =  products_2.loc[products_2.rating >10]
#Should be same as above can be reomoved
misfit_customers = list(set(sample['customerID']) - set(products_2['customerID']))

# Creates co-occurance matrix for itemsby item similartiy

stores = list(set(products_2['store_code']))

submission = pd.DataFrame()


s = 4843
cld2 = products_2[products_2['store_code'] == s]

train_data2 = graphlab.SFrame(data = cld2)
m = graphlab.item_similarity_recommender.create(train_data2, user_id = 'customerID' , item_id = 'product_code', target='rating',only_top_k=20)

customerIDs = list(set(cld2['customerID']))
makeitastring = []
defaultValueList = [None]*20

# For each customers get his personla recommendation and item item simillarity of what he is buying
for x in customerIDs:
    
    cld2 = products_2[products_2['customerID'] == x]
    cld2['rating'] = cld2.groupby('product_code')['product_code'].transform('count')
    sf = graphlab.SFrame(data=cld2)
    #Based on his own Recommendation
    mf = graphlab.factorization_recommender.create(sf, user_id = 'customerID' , item_id = 'product_code', target='rating')
    # Selected Top 3 from his personal recommendation and from total recommendation
    personalRecommdation = mf.recommend_from_interactions([], k=3)
    
    
    cluster_Personal = []
    cluster_Personal = personalRecommdation['product_code']
    #print (cluster_Personal)
    sa = SArray(data=cluster_Personal)
    totalRecommdation = m.get_similar_items(items=sa, k=3, verbose=False)
    #totalRecommdation = m.recommend([x], k=3)
    cluster_Recommdation = []
    cluster_Recommdation = totalRecommdation['similar']
    print(cluster_Personal)
    print(cluster_Recommdation)
    #print (cluster_Recommdation)
    empty = []
    empty.append(cluster_Recommdation)
    empty.append(cluster_Personal)
    for s in range (0, len(empty)):
        defaultValueList[s]= empty[s]
    makeitastring = ",".join(map(str, defaultValueList))
    print (makeitastring)
    d1 = { 'customerID' : x, 'products' : makeitastring}
    submission = submission.append(d1, ignore_index=True)    
    

        
#Default values from Sample, Actually these values should not get evaluated    
dealutValues = ['300663432','1000099534','1000475598'] 
for i in range(0, len(misfit_customers)):
    defaultValueList = [None] * 20
    for j in range(0, len(dealutValues)):
        defaultValueList[j] = dealutValues[j]
    makeitastring =  ",".join(map(str, defaultValueList))
    d = { 'customerID' : misfit_customers[i], 'products' : makeitastring}
    submission = submission.append(d, ignore_index=True)
    
    #submission_R3_p2 = submission_R3_p2.append(d, ignore_index=True)


submission.to_csv('../data/submission1.csv', index=False)
