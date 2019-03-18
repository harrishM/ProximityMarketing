import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

pd.options.mode.chained_assignment = None

#------------------------------IMPORT DATASET_________________________________
products = pd.read_csv('../data/cproducts.csv')
tender = pd.read_csv('../data/ctender.csv')
### check shape of files
print('product file has {} rows and {} columns'.format(products.shape[0], products.shape[1]))
print('tender file has {} rows and {} columns'.format(tender.shape[0], tender.shape[1]))
# this data file contains product level information of transactions made by customers
products.head()
tender.head()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DATA PREPROCESSING~~~~~~~~~~~~~~~~~~~~~~~

## fill missing values
products['promotion_description'].fillna('no_promo', inplace=True)
products['Gender'].fillna('no_gender', inplace=True)
products['State'].fillna('no_state', inplace=True)
products['PinCode'].fillna(-1, inplace=True)
products['DOB'].fillna("1", inplace=True)
#-----------------------------------CASH or DIGITAL MONEY ---------------------------
#Checkhing all transaction is also exist in tender file to distingusih digital money and money
productsMerged = products[products['transaction_number_by_till'].isin(tender['transaction_number_by_till'])]
productsNotIn= products[~products['transaction_number_by_till'].isin(tender['transaction_number_by_till'])]

#-------------------------------------------------------------------------------
#Droping duplicates and take last [To detect what payment currents he is using]
#---------------
out = tender.sort_values(['transaction_number_by_till', 'payment_amount_by_tender']).drop_duplicates(['transaction_number_by_till'], keep='last')

#CASH or Digital money
#cash = 1 Digital = 2
productsNotIn['PaymentUsed'] = 0
out.loc[out.PaymentUsed != 'CASH', 'PaymentUsed'] = 1
out.loc[out.PaymentUsed == 'CASH', 'PaymentUsed'] = 0

#Creating new dataset merged with product + with column 'PayementTypeUsed' from tender file
firstMergedColumn = out[['transaction_number_by_till' ,'PaymentUsed']]
secondMergedColumn = productsNotIn[['transaction_number_by_till' ,'PaymentUsed']]

mergedColumns = pd.merge(firstMergedColumn,secondMergedColumn ,  how ='outer') # Combine all tender transaction paymentused
mergedColumns.drop_duplicates(['transaction_number_by_till'])

merged1 = pd.merge(products, firstMergedColumn, on=['transaction_number_by_till'], how ='inner') # Transaction which is exist
merged2 = pd.merge(products, secondMergedColumn, on=['transaction_number_by_till'], how ='inner')  # Transaction may not exist
merged2 = products[products['transaction_number_by_till'].isin(secondMergedColumn['transaction_number_by_till'])] 
merged2['PaymentUsed'] = 0  #For missed out transaction by default CASH is added.
merged = pd.concat([merged1, merged2])
merged.drop_duplicates(['transaction_number_by_till'], keep='last') #Combine everything and keep last transaction he is used

#--------Above statemenet creates merged column along with products + payment used---------------------------

productsMerged = merged[merged['transaction_number_by_till'].isin(products['transaction_number_by_till'])]

#------- Distingusih male and female , By default values are assigned to male-------------------------------
productsMerged.loc[productsMerged.Gender != 'male', 'Gender'] = 1
productsMerged.loc[productsMerged.Gender == 'male', 'Gender'] = 0

#-------------Removing less importance to reduce menmory and increase speed---------------------------------
cluster_score = []
cld = productsMerged
cld.drop(['customerID','State','DOB','PinCode','transactionDate','store_description','till_no','transaction_number_by_till','promo_code','promotion_description','product_code','product_description','sale_price_after_promo','discountUsed'], axis=1, inplace=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DATA PREPROCESSING END~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Determing number of cluster~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
###  NUMBER OF STORES * PAYMENTTYPE USED(2) * GENDER (2)
stores = list(set(productsMerged['store_code']))
NumberofStore = len(stores)
k = NumberofStore * 2 * 2
print (k)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~KMENAS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
km1 = KMeans(n_clusters=k)
km2 = km1.fit(cld)
label = km2.predict(cld)
s_score = silhouette_score(cld, label)
cluster_score.append(s_score)


cluster_labels = []
cluster_store = []
cluster_data = []
cluster_customers = []

productsMerged = merged

# TO predict, change values as clustering
productsMerged.loc[productsMerged.Gender != 'male', 'Gender'] = 1
productsMerged.loc[productsMerged.Gender == 'male', 'Gender'] = 0
for x in stores:
    cld2 = productsMerged[productsMerged['store_code'] == x]
    cluster_customers.append(cld2['customerID'])
    cld2.drop(['customerID','State','DOB','PinCode','transactionDate','store_description','till_no','transaction_number_by_till','promo_code','promotion_description','product_code','product_description','sale_price_after_promo','discountUsed'], axis=1, inplace=True)
    #rbs = RobustScaler()
    #cld2 = rbs.fit_transform(cld2)
    label = km2.predict(cld2)
    cluster_labels.append(label)
    cluster_store.append(np.repeat(x, cld2.shape[0]))
    cluster_data.append(cld2)

cluster_data = np.concatenate(cluster_data)
cluster_data.shape   
cluster_customers = np.concatenate(cluster_customers)
cluster_store = np.concatenate(cluster_store)
cluster_labels = np.concatenate(cluster_labels)

sub1 = pd.DataFrame({'customerID':cluster_customers, 'store_code':cluster_store, 'cluster':cluster_labels})
np.savetxt('../data/subOne_18.txt', cluster_data)
sub1.to_csv('../data/subtwo_18.csv', index=False)
