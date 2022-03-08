from utils import getCSVData
from utils import Bases
from pprint import pprint
import airTableApi
from itertools import groupby

tripsBase = Bases.get('Trips')
# accountsTable = airTableApi.getTable(baseId=crmBase, tableName='All Accounts')
legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
aircraftTable = airTableApi.getTable(baseId=tripsBase, tableName='Aircraft')

airTableApi.linkTo(legsTable, aircraftTable, 100, 'aircraft_id', 'Aircraft Id', 'aircraft')

