import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv(r'C:\Users\mturnbow\OneDrive - Gainwell Technologies\Desktop\Direct Bill DL Res.csv')

def agg_check(dataframe):
    if dataframe['VERIF_LOAD_CHECK'].upper() == 'NOT LOADED TO VERIFICATIONS':
        return 'Not Loaded to Verifications'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'INVALID':
        return 'Verif Invalid'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'VERIFIED' and dataframe['FUS2NED_CHECK'].upper() == 'NOT MOVED TO NEDB':
        return 'Not Moved to NEDB'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'VERIFIED' and dataframe['FUS2NED_CHECK'].upper() == 'MOVED TO NEDB' and dataframe['MATCH_CHECK'].upper() == 'NOT MATCHED':
        return 'Not Matched'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'VERIFIED' and dataframe['FUS2NED_CHECK'].upper() == 'MOVED TO NEDB' and dataframe['MATCH_CHECK'].upper() == 'MATCHED' and dataframe['BILLED_CHECK'].upper() == 'NOT BILLED':
        return 'Not Billed'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'VERIFIED' and dataframe['FUS2NED_CHECK'].upper() == 'MOVED TO NEDB' and dataframe['MATCH_CHECK'].upper() == 'MATCHED' and dataframe['BILLED_CHECK'].upper() == 'BILLED':
        return 'Billed'
    elif dataframe['VERIF_LOAD_CHECK'].upper() == 'LOADED TO VERIFICATIONS' and dataframe['VERIF_CHECK'].upper() == 'VERIFIED' and dataframe['FUS2NED_CHECK'].upper() == 'MOVED TO NEDB' and dataframe['MATCH_CHECK'].upper() == 'MATCHED' and dataframe['BILLED_CHECK'].upper() == 'BILLED' and dataframe['RECOVERY_CHECK'].upper() == 'RECOVERED':
        return 'Recovered'
    else:
        return 'ERROR'

data['AGG_CHECK'] = data.apply(agg_check, axis=1)

pivot = data.pivot_table(index='AGG_CHECK', values='POSTING_ID', aggfunc='count')
print(pivot.columns)
print(pivot)

nlv = data.loc[data['AGG_CHECK'] == 'Not Loaded to Verifications'].count()[0]
vinv = data.loc[data['AGG_CHECK'] == 'Verif Invalid'].count()[0]
nonedb = data.loc[data['AGG_CHECK'] == 'Not Moved to NEDB'].count()[0]
nomatch = data.loc[data['AGG_CHECK'] == 'Not Matched'].count()[0]
nobill = data.loc[data['AGG_CHECK'] == 'Not Billed'].count()[0]
bill = data.loc[data['AGG_CHECK'] == 'Billed'].count()[0]
recover = data.loc[data['AGG_CHECK'] == 'Recovered'].count()[0]
err = data.loc[data['AGG_CHECK'] == 'ERROR'].count()[0]

print('Totals Gathered')

labels = ['Not Loaded to Verifications', 'Verification Invalid', 'Not Moved to NEDB', 'Not Mattched', 'Not Billed', 'Billed', 'Recovered', 'ERROR']
counts = [nlv, vinv, nonedb, nomatch, nobill, bill, recover, err]

########### Pie Chart Configuration
# plt.pie(counts, labels = labels, autopct='%.2f %%')

########## Bar Chart Configuration
ypos = np.arange(len(labels))
plt.yticks(ypos, labels)
plt.barh(ypos, counts)


plt.tight_layout()
plt.title('Direct Bill Data Gaps: Failure Point Distribution')

plt.show()
