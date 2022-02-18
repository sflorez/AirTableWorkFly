import requests
from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
from pprint import pprint

baseId = 'appamPDLTB6vyeNDB'
fbosTableName = 'FBOs'
airportsTableName = 'Airports'

fbosTable = airTableApi.getTable(baseId=baseId, tableName=fbosTableName)
airportsTable = airTableApi.getTable(baseId=baseId, tableName=airportsTableName)

airTableApi.linkTo(fbosTable, airportsTable, 100, "airport_id", "airport_id_copy", "Airport")