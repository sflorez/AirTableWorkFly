from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
from utils import Bases
import csv
from pprint import pprint

baseId = Bases.get('Sales')
quotesTableName = 'Quotes'
tripsTableName = 'Trips'

quotesTable = airTableApi.getTable(baseId=baseId, tableName=quotesTableName)
tripsTable = airTableApi.getTable(baseId=baseId, tableName=tripsTableName)

airTableApi.linkTo(quotesTable, tripsTable, 100, "Trip Number", "trip_number", "Trip")