import codecs
import csv
import json

Bases = {
         }

def writeJsonData(json_object, file_name):
    with open(f'{file_name}.json', 'w') as json_file:
        json.dump(json_object, json_file)

def readJsonData(file_name):
    try:
        with open(f'{file_name}.json', 'r') as json_file:
            json_object = json.load(json_file)
            return json_object
    except FileNotFoundError:
        print('no data file found, creating a new one...')
        return {}

def getCSVData(filepath, withCodec, delimiter):
    data = []
    if withCodec:
        with codecs.open(filepath, encoding=withCodec, errors='replace') as csvfile:
            rows = csv.DictReader(csvfile, delimiter=delimiter)
            for row in rows:
                data.append(row)
    else:
        with open(filepath) as csvfile:
            rows = csv.DictReader(csvfile, delimiter=delimiter)
            for row in rows:
                data.append(row)
    return data


    
