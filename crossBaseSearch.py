import airTableApi
from utils import Bases, TestBases
from pprint import pprint

financeBase = Bases.get('Finance')
tripsBase = Bases.get('Trips')

legsTable = airTableApi.getTable(baseId=tripsBase, tableName='Legs')
transactionsTable = airTableApi.getTable(baseId=financeBase, tableName='Transactions')

flights = [f['fields'].get('avianis_id') for f in legsTable.all(max_records= 100)]
allTransactions = transactionsTable.all()

expenses = list(filter(lambda x: [f for f in flights if f == x['fields'].get('Trip Leg Avianis ID')], allTransactions))
