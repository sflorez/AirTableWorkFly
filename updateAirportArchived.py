from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
import csv
from utils import Bases, getCSVData
from pprint import pprint
from itertools import groupby

base = Bases.get('Static')

airportsTable = airTableApi.getTable(baseId=base, tableName='Airports')

data = getCSVData(filepath='isArchivedAirport.csv', withCodec=None, delimiter=',')

allAirports = airportsTable.all()

recordsToUpdate = []
for row in data:
    record = [el for el in allAirports if str(el['fields'].get('airport_id_copy')) == row.get('id')]
    if record:
        recordsToUpdate.append({'id':record[0].get('id'), 'fields':{'is_archived' : row.get('is_archived')}})

pprint(len(recordsToUpdate))
airportsTable.batch_update(recordsToUpdate)