from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
import csv
from utils import Bases
from pprint import pprint
from itertools import groupby

baseId = 'appDD5U8qt2zgzwdq'
contactsTableName = 'Contacts'

contactTable = airTableApi.getTable(baseId=baseId, tableName=contactsTableName)

JCMergerData = []
with open('JCAVMerger.csv') as csvfile:
    merger = csv.DictReader(csvfile, delimiter=',')
    for row in merger:
        JCMergerData.append(row)

# pprint(JCMergerData[1:5])

allContacts = contactTable.all()
#pprint(allContacts[1:5])
recordsToUpdate = []
recordsToCreate = []
for row in JCMergerData:
    referenceRecords = [el for el in allContacts if el['fields'].get('avianis_id') == row.get('avianis_id')]
    if referenceRecords:
        recordsToUpdate.append({'id' : referenceRecords[0].get('id'), 'fields' : {'from_import' : 'true', 'contact_primary_id' : row.get('contact_primary_id'), 'avianis_primary_id' : row.get('avianis_primary_id'), 'contact_customer_id' : row.get('contact_customer_id'), 'avianis_customer_id' : row.get('avianis_customer_id')}})
    else:
        row['from_import'] = 'true'
        recordsToCreate.append(row)

pprint(len(recordsToUpdate))
pprint(len(recordsToCreate))
# pprint('about to delete...')
# pprint(len(recordsToUpdate))
sortedUpdate=sorted(recordsToUpdate, key=lambda record: (record['id'], -sum(1 for v in record.values() if v)))
uniqueRecordsToUpdate=[next(record) for _,record in groupby(sortedUpdate, key=lambda _d: _d['id'])]

pprint(len(uniqueRecordsToUpdate))

# pprint(len(uniqueRecordsToUpdate))
#pprint(recordsToCreate[1:5])
# recordsToDelete = list(map(lambda x: x.get('id'), uniqueRecordsToUpdate))
# pprint(len(uniqueRecordsToUpdate))
# pprint(len(recordsToDelete))
contactTable.batch_update(uniqueRecordsToUpdate)
contactTable.batch_create(recordsToCreate)