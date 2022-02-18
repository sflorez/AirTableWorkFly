import requests
from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
from pprint import pprint

baseId = 'apphXonxcXMIfE8qm'
quotesTableName = 'Quotes 1/2'
accountsTableName = 'All Accounts'

quotesTable = airTableApi.getTable(baseId=baseId, tableName=quotesTableName)
accountsTable = airTableApi.getTable(baseId=baseId, tableName=accountsTableName)

for records in quotesTable.iterate(page_size=10, max_records=100, view="the view"):
    recordsToUpdate = []
    for record in records:
        recordAccountName = record['fields']['account']
        formula = match({"account_name": recordAccountName})
        accountsRecord = accountsTable.first(formula=formula , fields=["Name"])
        if accountsRecord:
            idToLink = accountsRecord['id']
            recordsToUpdate.append({'fields': {'Accounts': [idToLink]}, 'id': record['id'] })
    quotesTable.batch_update(recordsToUpdate)

