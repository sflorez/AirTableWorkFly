from utils import getCSVData
from utils import Bases
from pprint import pprint
import airTableApi
from itertools import groupby

tripsBase = Bases.get('Trips')
# accountsTable = airTableApi.getTable(baseId=crmBase, tableName='All Accounts')
legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
tripsTable = airTableApi.getTable(baseId=tripsBase, tableName='Trips')
accountsTable = airTableApi.getTable(baseId=tripsBase, tableName='Accounts')
newLegs = getCSVData('legs (1).csv', withCodec=None, delimiter=',')
allLegs = legsTable.all()
# allAccounts = accountsTable.all()
recordsToUpdate = []
recordsToCreate = []

# pprint(len(newLegs))

# for leg in newLegs:
#     recordsToCreate.append(leg)

# def keyRemapping(x):
#     return {"leg_id" if k == 'id' else k:v for k,v in x.items()} 

# idRemappedData = list(map(keyRemapping, newLegs))

# for newleg in idRemappedData:
#     matchedLeg = [el for el in allLegs if el['fields'].get('leg_id') == newleg.get('leg_id')]
#     if matchedLeg:
#         pprint('do nothing')
#     else:
#         recordsToCreate.append(newleg)
# for trip in allTrips:
#     matchedAccount = [el for el in allAccounts if el['fields'].get('customer_id') == trip['fields'].get('customer_id')]
#     # pprint(leg['fields'].get('customer_id (from Trip)'))
#     # pprint(matchedAccount)
#     if matchedAccount:
#         recordsToUpdate.append({'id': trip.get('id'), 'fields' : {'Account' : [matchedAccount[0].get('id')]}})
#     else:
#         avMatchedAccount = [el for el in allAccounts if el['fields'].get('av_customer_id') == trip['fields'].get('customer_id')]
#         # pprint(avMatchedAccount)
#         if avMatchedAccount:
#             recordsToUpdate.append({'id': trip.get('id'), 'fields' : {'Account' : [avMatchedAccount[0].get('id')]}})

# pprint(len(recordsToUpdate))
# pprint(len(recordsToCreate))
# legsTable.batch_create(recordsToCreate, typecast=True)