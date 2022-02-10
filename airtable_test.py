import traceback
import requests
import csv
import json
from pyairtable import Table
from pprint import pprint

def keyRemapping(x):
    return {"quote_id" if k == 'id' else k:v for k,v in x.items()}   
    
api_key = "keyCLQeVtRKMNN6m4"
table = Table(api_key, 'apphXonxcXMIfE8qm', 'Quotes 2/2')

quotesData = []
with open('allQuotes.csv') as csvfile:
    quotes = csv.DictReader(csvfile, delimiter=',')
    for row in quotes:
        quotesData.append(row)

formattedData = list(map(keyRemapping, quotesData[50000:len(quotesData)]))
betterData = []
for row in formattedData:
    for key, value in row.items():
        try:
            if(key == 'account'):
                continue
            else:
                row[key] == float(value)
        except:
            continue
    betterData.append(row)

try:
    table.batch_create(betterData, typecast=True)
except:
    traceback.print_exc()


