import urllib.request, json
import pandas as pd

npi_num_list = [ENTER NPI LIST FOR LOOKUP HERE]


headers = ['NATIONAL_PROVIDER_ID','ORGANIZATION_NM','PROVIDER_LAST_NM','PROVIDER_FIRST_NM']
df_main = pd.DataFrame(columns= headers)

def get_npi_info(npi_num):
    url = urllib.request.urlopen(
        "https://npiregistry.cms.hhs.gov/api/?version=2.1&number=" + npi_num)
    provider_data = json.loads(url.read())
    return provider_data

for number in npi_num_list:
    data = get_npi_info(number)
    df_single = pd.DataFrame(columns = headers)
    results = data['results'][0]
    npi = results['number']
    org_name = results['basic']['organization_name']
    last_nm = results['basic']['authorized_official_last_name']
    first_nm = results['basic']['authorized_official_first_name']

    df_single.loc[0] = [npi, org_name, last_nm, first_nm]
    df_main = pd.concat([df_main, df_single], ignore_index=True)

print(df_main)
df_main.to_csv(IF NEEDED - ENTER EXPORT FILE LOCATION)

