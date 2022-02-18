import requests
from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
from pprint import pprint

baseId = 'apphXonxcXMIfE8qm'
quotesTableName = 'Quotes'
quoteLegsTableName = 'Quote Legs'

quotesTable = airTableApi.getTable(baseId=baseId, tableName=quotesTableName)
quotesLegTable = airTableApi.getTable(baseId=baseId, tableName=quoteLegsTableName)

airTableApi.linkTo(quotesLegTable, quotesTable, 100, "QuoteGuidId", "QuoteGuidId", "Quotes Link")
    