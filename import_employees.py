import traceback
import requests
import csv
import json
import airTableApi
from pprint import pprint

def keyRemapping(x):
    return {"employee_id" if k == 'id' else k:v for k,v in x.items()}

tableId = 'apppywY8da44n71Ee'
tableName = 'Employees'

tableInstance = airTableApi.getTable(baseId=tableId, tableName=tableName)

employeeData = []
with open('allemployees.csv') as csvfile:
    employees = csv.DictReader(csvfile, delimiter=',')
    for row in employees:
        employeeData.append(row)

idRemappedData = list(map(keyRemapping, employeeData))

# pprint(idRemappedData)

try:
    tableInstance.batch_create(idRemappedData, typecast=True)
except:
    traceback.print_exc()