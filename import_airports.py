import traceback
import requests
import csv
import json
import airTableApi
from pprint import pprint

def keyRemapping(x):
    return {"airport_id" if k == 'id' else k:v for k,v in x.items()} 

tableId = 'appamPDLTB6vyeNDB'
tableName = 'Airports'

tableInstance  = airTableApi.getTable(baseId=tableId, tableName=tableName)

airportData = []
with open('allAirports.csv') as csvfile:
    airports = csv.DictReader(csvfile, delimiter=',')
    for row in airports:
        airportData.append(row)

idRemappedData = list(map(keyRemapping, airportData))

# pprint(idRemappedData)

try:
    tableInstance.batch_create(idRemappedData, typecast=True)
except:
    traceback.print_exc()