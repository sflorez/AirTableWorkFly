from utils import getCSVData
from utils import Bases
from pprint import pprint
import airTableApi
from itertools import groupby

tripsBase = Bases.get('Trips')
# accountsTable = airTableApi.getTable(baseId=crmBase, tableName='All Accounts')
legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
airportsTable = airTableApi.getTable(baseId=tripsBase, tableName='Airports')

airTableApi.linkTo(legsTable, airportsTable, 100, 'arrival_airport_icao', 'airport_id', 'arrival_airport')

