from utils import getCSVData
from utils import Bases
from pprint import pprint
import airTableApi
from itertools import groupby

tripsBase = Bases.get('Trips')
# accountsTable = airTableApi.getTable(baseId=crmBase, tableName='All Accounts')
legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
fbosTable = airTableApi.getTable(baseId=tripsBase, tableName='FBOs')

airTableApi.linkTo(legsTable, fbosTable, 100, 'departure_fbo_formula', 'fbo_id', 'departure_fbo')

