import traceback
import csv
import airTableApi
import codecs
from pprint import pprint

def keyRemapping(x):
    return {"address_id" if k == 'id' else k:v for k,v in x.items()} 

baseId = 'apppywY8da44n71Ee'
tableName = 'Addresses'
types_of_encoding = ["utf8"]

addressesTable = airTableApi.getTable(baseId=baseId, tableName=tableName)

addressData = []
for encoding_type in types_of_encoding:
    with codecs.open('allAddresses.csv', encoding=encoding_type, errors='replace') as csvfile:
        addresses = csv.DictReader(csvfile, delimiter=',')
        for row in addresses:
            addressData.append(row)

idRemappedData = list(map(keyRemapping, addressData))

# pprint(idRemappedData)

try:
    addressesTable.batch_create(idRemappedData, typecast=True)
except:
    traceback.print_exc()