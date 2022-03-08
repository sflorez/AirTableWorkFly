from email.mime import base
from utils import getCSVData
from utils import Bases
from pprint import pprint
import airTableApi
from itertools import groupby

data = getCSVData('legs.csv', None, ',')

# pprint(data)
# pprint(len(data))

crmBase = Bases.get('CRM')
tripsBase = Bases.get('Trips')
# accountsTable = airTableApi.getTable(baseId=crmBase, tableName='All Accounts')
legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
tripsTable = airTableApi.getTable(baseId=tripsBase, tableName='Trips')
accountsTable = airTableApi.getTable(baseId=tripsBase, tableName='Accounts')

# alltrips = tripsTable.all(view='all trips')
# allAccounts = accountsTable.all()
recordsToUpdate = []
pprint(tripsTable.fields)
# pprint(allAccounts[0:10])

# for leg in alllegs:
#     matchedAccount = [el for el in allAccounts if el['fields'].get('customer_id') == leg['fields'].get('customer_id (from Trip)')[0]]
#     # pprint(leg['fields'].get('customer_id (from Trip)'))
#     # pprint(matchedAccount)
#     if matchedAccount:
#         recordsToUpdate.append({'id': leg['fields'].get('Trip')[0], 'fields' : {'Account' : [matchedAccount[0].get('id')]}})
#     else:
#         avMatchedAccount = [el for el in allAccounts if el['fields'].get('av_customer_id') == leg['fields'].get('customer_id (from Trip)')[0]]
#         # pprint(avMatchedAccount)
#         if avMatchedAccount:
#             recordsToUpdate.append({'id': leg['fields'].get('Trip')[0], 'fields' : {'Account' : [avMatchedAccount[0].get('id')]}})

# pprint(len(recordsToUpdate))
# pprint(recordsToUpdate[0:5])

# sortedUpdate=sorted(recordsToUpdate, key=lambda record: (record['id'], -sum(1 for v in record.values() if v)))
# uniqueRecordsToUpdate=[next(record) for _,record in groupby(sortedUpdate, key=lambda _d: _d['id'])]

# pprint(len(uniqueRecordsToUpdate))
# pprint(uniqueRecordsToUpdate[0:5])
# tripsTable.batch_update(uniqueRecordsToUpdate)


    # matchedAccounts = list(map(lambda x: x['fields'].get('Name'), (filter(lambda x: [el for el in allAccounts if el == x['fields'].get('customer_id')], allAccounts))))
    # matchedAVOnly = list(map(lambda x: x['fields'].get('Name'), (filter(lambda x: [el for el in data if el == x['fields'].get('av_customer_id')], allAccounts))))








# # pprint(matchedAccounts)
# pprint(len(matchedAccounts))

# pprint(len(matchedAVOnly))
# # pprint(len(matchedAVOnly))

# # pprint(set(matchedAccounts + matchedAVOnly))
# pprint(len(set(matchedAVOnly + matchedAccounts)))