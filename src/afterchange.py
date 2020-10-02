

import pandas as pd
import warnings
warnings.filterwarnings('ignore')




#print(df1['transactionDate'].min())


#print(df1['transactionDate'].max())
def segmentation(df1):
    import datetime as dt
    NOW = dt.datetime(2017,7,1)
    
    df1['transactionDate'] = pd.to_datetime(df1['transactionDate'])
    rfmTable = df1.groupby('customer').agg({'transactionDate': lambda x: (NOW - x.max()).days, # Recency
                                            'transaction_number_by_till': lambda x: len(x),      # Frequency
                                            'sale_price_after_promo': lambda x: x.sum()}) # Monetary Value
    
 
    rfmTable['transactionDate'] = rfmTable['transactionDate'].astype(int)
    rfmTable.rename(columns={'transactionDate': 'recency', 
                             'transaction_number_by_till': 'frequency', 
                             'sale_price_after_promo': 'monetary_value'}, inplace=True)
    #print(rfmTable.head())
    
    first_customer = df1[df1['customer']== 'BBID_211419317']
    first_customer
    
    #(NOW -dt.datetime(2017,6,6)).days==326
    
    quantiles = rfmTable.quantile(q=[0.25,0.5,0.75])
    quantiles
    quantiles = quantiles.to_dict()
    quantiles
    segmented_rfm = rfmTable
    
    def RScore(x,p,d):
        if x <= d[p][0.25]:
            return 1
        elif x <= d[p][0.50]:
            return 2
        elif x <= d[p][0.75]: 
            return 3
        else:
            return 4
        
    def FMScore(x,p,d):
        if x <= d[p][0.25]:
            return 4
        elif x <= d[p][0.50]:
            return 3
        elif x <= d[p][0.75]: 
            return 2
        else:
            return 1
        
    
    
    segmented_rfm['R_Quartile'] = segmented_rfm['recency'].apply(RScore, args=('recency',quantiles,))
    segmented_rfm['F_Quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency',quantiles,))
    segmented_rfm['M_Quartile'] = segmented_rfm['monetary_value'].apply(FMScore, args=('monetary_value',quantiles,))
    
    segmented_rfm.head()
    segmented_rfm['RFM'] = segmented_rfm.R_Quartile.map(str) \
                                + segmented_rfm.F_Quartile.map(str) \
                                + segmented_rfm.M_Quartile.map(str)
    
    segmented_rfm.head()
   
    #print(segmented_rfm[segmented_rfm['RFMScore']=='111'].sort_values('monetary_value', ascending=False).head(10))
    
    #print(sum(segmented_rfm['monetary_value']))
    pareto_cutoff = 0.8 * sum(segmented_rfm['monetary_value'])
    print (pareto_cutoff)
    s = segmented_rfm.sort_values('monetary_value', ascending=False)
    s1 = s.head(round((0.30 *len(s))))
    print (sum(s1['monetary_value']))
    segmented_rfm = segmented_rfm.sort_values('monetary_value', ascending=False)
    segmented_rfm['topcustomers'] = 0
    segmented_rfm.loc[:round(0.35 *len(s)),'topcustomers']=1
    #segmented_rfm = segmented_rfm.reindex(segmented_rfm.index.rename(['customerID']))
    return segmented_rfm
#print(pareto_cutoff)

 
df = pd.read_csv("../data/cproducts.csv")
df.head()
df1 = df

df1 = df1.rename(columns={'customerID': 'customer'})
df1['promotion_description'].fillna('no_promo', inplace=True)
df1['Gender'].fillna('no_gender', inplace=True)
df1['State'].fillna('no_state', inplace=True)
df1['PinCode'].fillna(-1, inplace=True)
df1['DOB'].fillna("1", inplace=True)
#Store location

df1.drop_duplicates(['transaction_number_by_till'], keep='last')

df1.groupby(['store_code'])['customer'].aggregate('count').reset_index().sort_values('customer', ascending=False)

#df1 = df1.loc[df1['store_code'] == '2655']
#df1 = df1[pd.notnull(df1['customerID'])]
#df1 = df1[pd.notnull(df1['customerID'])]
#df1.Quantity.min()
#df1.Quantity.max()
completeList = pd.DataFrame()
newDF = pd.DataFrame()
stores = list(set(df1['store_code']))
for x in stores:
    cld2 = df1[df1['store_code'] == x]
    s = segmentation(cld2)
    newDF = newDF.append(s)
    merged1 = pd.merge(cld2, s, left_on='customer', right_index=True, how ='inner')
    completeList = completeList.append(merged1)
    completeList.drop(['Gender','store_code','store_description','DOB','State','PinCode','transactionDate','till_no','transaction_number_by_till','promo_code','promotion_description','product_code','product_description','sale_price_after_promo','discountUsed'], axis=1, inplace=True)
    
    completeList.to_csv("../data/output1"+ str(x) +".csv", index=False)

completeList.to_csv("../data/output.csv", index=False)
df1.head()

#df1['sale_price_after_promo'] = df1['Quantity'] * df1['UnitPrice']
df1.head()

