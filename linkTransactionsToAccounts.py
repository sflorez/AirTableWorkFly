from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
import csv
from pprint import pprint

baseId = 'appl5oSDrBVZ9geuj'
transactionsTableName = 'Transactions'
accountsTableName = 'All Accounts'

transactionsTable = airTableApi.getTable(baseId=baseId, tableName=transactionsTableName)
accountsTable = airTableApi.getTable(baseId=baseId, tableName=accountsTableName)

airTableApi.linkTo(transactionsTable, accountsTable, 100, "Customer ID copy", "customer_id", "Accounts Link")