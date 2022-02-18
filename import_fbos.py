import traceback
import requests
import csv
import json
import airTableApi
from pprint import pprint
import codecs

def keyRemapping(x):
    return {"fbo_id" if k == 'id' else k:v for k,v in x.items()}

types_of_encoding = ["utf8"]
tableId = 'appamPDLTB6vyeNDB'
tableName = 'FBOs'

tableInstance = airTableApi.getTable(baseId=tableId, tableName=tableName)

fboData = []

for encoding_type in types_of_encoding:
    with codecs.open('allfbos.csv', encoding= encoding_type, errors='replace') as csvfile:
        fbos = csv.DictReader(csvfile, delimiter=',')
        for row in fbos:
            fboData.append(row)

idRemappedData = list(map(keyRemapping, fboData))

# pprint(idRemappedData)

try:
    tableInstance.batch_create(idRemappedData, typecast=True)
except:
    traceback.print_exc()