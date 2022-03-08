from lib2to3.pytree import Base
from weakref import ref
import airTableApi
from utils import getCSVData
from utils import Bases, TestBases
from pprint import pprint

base = Bases.get('CRM')

data = getCSVData('airtable_accounts.csv', withCodec=None, delimiter=',')

accountsTable = airTableApi.getTable(baseId=base, tableName='All Accounts')

allAccounts = accountsTable.all()

# accountNames = set([el['fields'].get('Name') for el in allAccounts])
# psAccountName = set([el.get('account_name') for el in data])
# pprint(len(accountNames))
pprint(len(data))
pprint(len(allAccounts))

recordsToUpdate = []
recordsToCreate = []

for row in data:
    referenceRecord = [el for el in allAccounts if el['fields'].get('Name') == row.get('account_name')]
    if referenceRecord:
        if referenceRecord[0]['fields'].get('email'):
            row['email'] = referenceRecord[0]['fields'].get('email')
        if referenceRecord[0]['fields'].get('work_phone'):
            row['work_phone'] = referenceRecord[0]['fields'].get('work_phone')
        recordsToUpdate.append({'id' : referenceRecord[0].get('id'), 'fields' : row})
    else:
        recordsToCreate.append(row)

pprint(len(recordsToUpdate))
pprint(len(recordsToCreate))

accountsTable.batch_update(recordsToUpdate)
accountsTable.batch_create(recordsToCreate)



# pprint(len(accountNames - psAccountName))