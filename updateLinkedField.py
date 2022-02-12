import requests
from pyairtable import Table
import airTableApi
from pprint import pprint

tableId = 'apphXonxcXMIfE8qm'
tableName = 'Quotes 1/2'

tableInstance = airTableApi.getTable(tableId=tableId, tableName=tableName)

for records in tableInstance.iterate(page_size=10, max_records=10):
    pprint(records)