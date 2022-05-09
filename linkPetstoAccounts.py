import requests
from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
from pprint import pprint

baseId = 'apppywY8da44n71Ee'
petsTableName = 'Pets'
accountsTableName = 'All Accounts'

petsTable = airTableApi.getTable(baseId=baseId, tableName=petsTableName)
accountsTable = airTableApi.getTable(baseId=baseId, tableName=accountsTableName)

airTableApi.linkTo(petsTable, accountsTable, 100, "customer_id", "jet_club_customer_id", "Accounts Link")


# for records in petsTable.iterate(page_size=100):
#     recordsToUpdate = []
#     for record in records:
#         recordPetCustomerId = record['fields'].get("customer_id")
#         if recordPetCustomerId:
#             formula = match({'customer_id' : recordPetCustomerId})
#             # pprint(recordPetCustomerId)
#             accountsRecord = accountsTable.first(formula=formula)
#             # pprint(accountsRecord)
#             if accountsRecord:
#                 idToLink = accountsRecord['id']
#                 # pprint(idToLink)
#                 recordsToUpdate.append({'id' : record['id'], 'fields' : {'Accounts Link' : [idToLink]}})
#     # pprint(recordsToUpdate)
#     petsTable.batch_update(recordsToUpdate)