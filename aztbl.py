from azure.data.tables import TableServiceClient
import os
import pandas as pd
import datetime

connstr = connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
service = TableServiceClient.from_connection_string(conn_str=connstr)

#print(dir(service))
print(service.list_tables())

df = pd.read_csv('data/All_covid_20220103.csv', parse_dates=["Date"], dayfirst=True, infer_datetime_format=True)
print(df.shape)
print(df.dtypes)
print(df.head)

df_unpivot = df.melt(id_vars='Date', var_name='State', value_name='New_cases')
print(df_unpivot.shape)
print(df_unpivot.tail())

df_unpivot.to_csv('data/covid_denorm.csv', index=False)
df_covid_aztbl = df_unpivot.rename(columns={'State': 'PartitionKey', 'Date': 'RowKey'})
#df_covid_aztbl['PartitionKey'] = df_covid_aztbl

def setTable(aztblsrvobj, table_name, ent_lstofdict):
    """ add entities to azure table
    """

    table_client = aztblsrvobj.get_table_client(table_name)

    for row in ent_lstofdict:
        print(f"adding row {row}")
        try:
            entity = table_client.create_entity(entity=row)
        except Exception as err:
            print(err)
        #print(entity)


TABLE_NAME = 'tblauscovid19'

coviddict = df_covid_aztbl.to_dict('r')
print(coviddict[:5])

#setTable(service, TABLE_NAME, coviddict)

tableclient = service.get_table_client(TABLE_NAME)
# print(dir(tableclient))
testdict = {'RowKey': '04_12', 'PartitionKey': 'NSW', 'New_cases': '325'}
#tableclient.create_entity(testdict)

def convertutcdate(indate):
    """ Convert UTC date
    """

    spltdate = indate.split('/')
    if int(spltdate[1]) == 12:
        spltdate.append('2021')
    else:
        spltdate.append('2022')
    newdate = '-'.join(spltdate)
    return newdate

print(convertutcdate('04/12'))
print(convertutcdate('04/01'))